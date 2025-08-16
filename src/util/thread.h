/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_THREAD_H_
#define UTIL_THREAD_H_

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <queue>
#include <vector>
#include <sys/eventfd.h>

class Mutex{
	private:
		pthread_mutex_t mutex;
	public:
		Mutex(){
			pthread_mutex_init(&mutex, NULL);
		}
		~Mutex(){
			pthread_mutex_destroy(&mutex);
		}
		void lock(){
			pthread_mutex_lock(&mutex);
		}
		void unlock(){
			pthread_mutex_unlock(&mutex);
		}
};

class Locking{
	private:
		Mutex *mutex;
		// No copying allowed
		Locking(const Locking&);
		void operator=(const Locking&);
	public:
		Locking(Mutex *mutex){
			this->mutex = mutex;
			this->mutex->lock();
		}
		~Locking(){
			this->mutex->unlock();
		}

};

/*
class Semaphore {
	private:
		pthread_cond_t cond;
		pthread_mutex_t mutex;
	public:
		Semaphore(Mutex* mu){
			pthread_cond_init(&cond, NULL);
			pthread_mutex_init(&mutex, NULL);
		}
		~CondVar(){
			pthread_cond_destroy(&cond);
			pthread_mutex_destroy(&mutex);
		}
		void wait();
		void signal();
};
*/


// Thread safe queue
template <class T>
class Queue{
	private:
		pthread_cond_t cond;
		pthread_mutex_t mutex;
		std::queue<T> items;
	public:
		Queue();
		~Queue();

		bool empty();
		int size();
		int push(const T item);
		// TODO: with timeout
		int pop(T *data);
};


// Selectable queue, multi writers, single reader
template <class T>
class SelectableQueue{
	private:
		int efd;
		pthread_mutex_t mutex;
		std::queue<T> items;
	public:
		SelectableQueue();
		~SelectableQueue();
		int fd(){
			return efd;
		}
		int size();
		// multi writer
		int push(const T item);
		// single reader
		int pop(std::vector<T> *data);
};

template<class W, class JOB>
class WorkerPool{
	public:
		class Worker{
			public:
				Worker(){};
				Worker(const std::string &name);
				virtual ~Worker(){}
				int id;
				virtual void init(){}
				virtual void destroy(){}
				virtual int proc(JOB job) = 0;
			private:
			protected:
				std::string name;
		};
	private:
		std::string name;
		Queue<JOB> jobs;
		SelectableQueue<JOB> results;

		int num_workers;
		std::vector<pthread_t> tids;
		bool started;

		struct run_arg{
			int id;
			WorkerPool *tp;
		};
		static void* _run_worker(void *arg);
	public:
		WorkerPool(const char *name="");
		~WorkerPool();

		int fd(){
			return results.fd();
		}
		
		int start(int num_workers);
		int stop();
		
		int push(JOB job);
		int pop(std::vector<JOB> *job);
};





template <class T>
Queue<T>::Queue(){
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
}

template <class T>
Queue<T>::~Queue(){
	pthread_cond_destroy(&cond);
	pthread_mutex_destroy(&mutex);
}

template <class T>
bool Queue<T>::empty(){
	bool ret = false;
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	ret = items.empty();
	pthread_mutex_unlock(&mutex);
	return ret;
}

template <class T>
int Queue<T>::size(){
	int ret = -1;
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	ret = items.size();
	pthread_mutex_unlock(&mutex);
	return ret;
}

template <class T>
int Queue<T>::push(const T item){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	items.push(item);
	pthread_mutex_unlock(&mutex);
	pthread_cond_signal(&cond);
	return 1;
}

template <class T>
int Queue<T>::pop(T *data){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	while(items.empty()){
		if(pthread_cond_wait(&cond, &mutex) != 0){
			return -1;
		}
	}
	*data = items.front();
	items.pop();
	pthread_mutex_unlock(&mutex);
	return 1;
}


template <class T>
SelectableQueue<T>::SelectableQueue(){
	efd = eventfd(0, EFD_NONBLOCK);
	if(efd == -1){
		fprintf(stderr, "create eventfd error\n");
		exit(0);
	}
	pthread_mutex_init(&mutex, NULL);
}

template <class T>
SelectableQueue<T>::~SelectableQueue(){
	pthread_mutex_destroy(&mutex);
	close(efd);
}

template <class T>
int SelectableQueue<T>::push(const T item){
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	bool need_signal = items.empty();
	items.push(item);
	pthread_mutex_unlock(&mutex);
	if (need_signal) {
		uint64_t one = 1;
		while(1){
			int n = ::write(efd, &one, sizeof(one));
			if(n < 0){
				if(errno == EINTR) continue;
				if(errno == EAGAIN) break;
				return -1;
			}
			break;
		}
	}
	return 1;
}

template <class T>
int SelectableQueue<T>::size(){
	int ret = 0;
	pthread_mutex_lock(&mutex);
	ret = items.size();
	pthread_mutex_unlock(&mutex);
	return ret;
}

template <class T>
int SelectableQueue<T>::pop(std::vector<T> *data){
	uint64_t cnt;
	while(1){
		int n = ::read(efd, &cnt, sizeof(cnt));
		if(n < 0){
			if(errno == EINTR) continue;
			if(errno == EAGAIN) break;
			return -1;
		}
		break;
	}
	if(pthread_mutex_lock(&mutex) != 0){
		return -1;
	}
	while(!items.empty()){
		data->push_back(items.front());
		items.pop();
	}
	pthread_mutex_unlock(&mutex);
	return 1;
}


template<class W, class JOB>
WorkerPool<W, JOB>::WorkerPool(const char *name){
	this->name = name;
	this->started = false;
}

template<class W, class JOB>
WorkerPool<W, JOB>::~WorkerPool(){
	if(started){
		stop();
	}
}

template<class W, class JOB>
int WorkerPool<W, JOB>::push(JOB job){
	return this->jobs.push(job);
}

template<class W, class JOB>
int WorkerPool<W, JOB>::pop(std::vector<JOB> *job){
	return this->results.pop(job);
}

template<class W, class JOB>
void* WorkerPool<W, JOB>::_run_worker(void *arg){
	struct run_arg *p = (struct run_arg*)arg;
	int id = p->id;
	WorkerPool *tp = p->tp;
	delete p;

	W w(tp->name);
	Worker *worker = (Worker *)&w;
	worker->id = id;
	worker->init();
	while(1){
		JOB job;
		if(tp->jobs.pop(&job) == -1){
			fprintf(stderr, "jobs.pop error\n");
			exit(0);
			break;
		}
		if(!tp->started){
			break;
		}
		worker->proc(job);
		if(tp->results.push(job) == -1){
			fprintf(stderr, "results.push error\n");
			exit(0);
			break;
		}
	}
	worker->destroy();
	// fprintf(stderr, "  %d stopped\n", id);
	return (void *)NULL;
}

template<class W, class JOB>
int WorkerPool<W, JOB>::start(int num_workers){
	this->num_workers = num_workers;
	if(started){
		return 0;
	}
	int err;
	pthread_t tid;
	for(int i=0; i<num_workers; i++){
		struct run_arg *arg = new run_arg();
		arg->id = i;
		arg->tp = this;

		err = pthread_create(&tid, NULL, &WorkerPool::_run_worker, arg);
		if(err != 0){
			fprintf(stderr, "can't create thread: %s\n", strerror(err));
		}else{
			tids.push_back(tid);
		}
	}
	started = true;
	return 0;
}

template<class W, class JOB>
int WorkerPool<W, JOB>::stop(){
	// notify and wait workers quit
	started = false;
	// notify
	for(int i=0; i<tids.size(); i++){
		JOB j = NULL;
		this->push(j);
	}
	// wait
	for(int i=0; i<tids.size(); i++){
		pthread_join(tids[i], NULL);
	}
	return 0;
}

#endif

