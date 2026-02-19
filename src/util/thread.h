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
#include <atomic>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <limits.h>
#include <stdint.h>
#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CPU_RELAX() _mm_pause()
#else
#define CPU_RELAX() do {} while (0)
#endif

static inline int futex_wait(int* addr, int expected) {
	return syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, NULL, NULL, 0);
}
static inline int futex_wake(int* addr, int n) {
	return syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, n, NULL, NULL, 0);
}

class Mutex {
private:
	pthread_mutex_t mutex;
public:
	Mutex() {
		pthread_mutex_init(&mutex, NULL);
	}
	~Mutex() {
		pthread_mutex_destroy(&mutex);
	}
	void lock() {
		pthread_mutex_lock(&mutex);
	}
	void unlock() {
		pthread_mutex_unlock(&mutex);
	}
};

class Locking {
private:
	Mutex* mutex;
	// No copying allowed
	Locking(const Locking&);
	void operator=(const Locking&);
public:
	Locking(Mutex* mutex) {
		this->mutex = mutex;
		this->mutex->lock();
	}
	~Locking() {
		this->mutex->unlock();
	}

};

// Thread safe queue
template <class T, size_t CAP> class Queue {
	static_assert((CAP& (CAP - 1)) == 0, "CAP must be power of two");
	static_assert(CAP <= (1ull << 63), "CAP must be < 2^63 for wrap-safe signed diffs");
private:
	struct alignas(64) Slot {
		std::atomic<uint64_t> seq;
		alignas(64) T item;
	};
	Slot ring_[CAP];

	alignas(64) std::atomic<uint64_t> tail_{ 0 };
	alignas(64) std::atomic<uint64_t> head_{ 0 };
public:
	Queue();

	void push(T item);
	bool pop(T* data);
};

template<class W, class JOB>
class WorkerPool {
public:
	class Worker {
	public:
		Worker() {};
		Worker(const std::string& name);
		virtual ~Worker() {}
		int id;
		virtual void init() {}
		virtual void destroy() {}
		virtual void proc(JOB* job) = 0;
	private:
	protected:
		std::string name;
	};
private:
	std::string name;
	Queue<JOB, (1 << 16)> jobs;

	int num_workers;
	std::vector<pthread_t> tids;
	bool started;

	struct run_arg {
		int id;
		WorkerPool* tp;
	};
	pthread_mutex_t mutex;
	alignas(64) int pending_work;
	static void* _run_worker(void* arg);
public:
	WorkerPool(const char* name = "");
	~WorkerPool();

	void start(int num_workers);
	void stop();

	void push(JOB& job);
};

template <class T, size_t CAP>
Queue<T, CAP>::Queue() {
	for (size_t i = 0; i < CAP; i++)
		ring_[i].seq.store(i, std::memory_order_relaxed);
}

template <class T, size_t CAP>
void Queue<T, CAP>::push(T item) {
	uint64_t pos = tail_.fetch_add(1, std::memory_order_relaxed);
	Slot& s = ring_[pos & (CAP - 1)];
	for (;;) {
		uint64_t seq = s.seq.load(std::memory_order_acquire);
		int64_t dif = (int64_t)(seq - pos);
		if (dif == 0) break;
		CPU_RELAX();
	}
	s.item = std::move(item);
	s.seq.store(pos + 1, std::memory_order_release);
}

template <class T, size_t CAP>
bool Queue<T, CAP>::pop(T* data) {
	uint64_t pos = head_.load(std::memory_order_relaxed);
	for (;;) {
		Slot& s = ring_[pos & (CAP - 1)];
		uint64_t seq = s.seq.load(std::memory_order_acquire);
		int64_t dif = (int64_t)(seq - (pos + 1));
		if (dif == 0) {
			if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_acq_rel,
				std::memory_order_relaxed)) {
				*data = std::move(s.item);
				s.seq.store(pos + CAP, std::memory_order_release);
				return true;
			}
			else {
				CPU_RELAX();
				continue;
			}
		}
		else if (dif < 0) {
			return false; // empty
		}
		else {
			CPU_RELAX();
			pos = head_.load(std::memory_order_relaxed);
		}
	}
}

template<class W, class JOB>
WorkerPool<W, JOB>::WorkerPool(const char* name) {
	pthread_mutex_init(&mutex, NULL);
	this->name = name;
	this->started = false;
	this->pending_work = 0;
}

template<class W, class JOB>
WorkerPool<W, JOB>::~WorkerPool() {
	stop();
	pthread_mutex_destroy(&mutex);
}

template<class W, class JOB>
void WorkerPool<W, JOB>::push(JOB& job) {
	jobs.push(std::move(job));
	pthread_mutex_lock(&mutex);
	bool need_wake = (pending_work == 0);
	pending_work = 1;
	if (need_wake)
		futex_wake(&pending_work, 1);
	pthread_mutex_unlock(&mutex);
}

template<class W, class JOB>
void* WorkerPool<W, JOB>::_run_worker(void* arg) {
	struct run_arg* p = (struct run_arg*)arg;
	int id = p->id;
	WorkerPool* tp = p->tp;
	delete p;

	W w(tp->name);
	Worker* worker = (Worker*)&w;
	worker->id = id;
	worker->init();
	while (1) {
		pthread_mutex_lock(&tp->mutex);
		while (tp->started && tp->pending_work == 0) {
			pthread_mutex_unlock(&tp->mutex);
			futex_wait(&tp->pending_work, 0);
			pthread_mutex_lock(&tp->mutex);
		}
		if (!tp->started && tp->pending_work == 0) {
			pthread_mutex_unlock(&tp->mutex);
			break;
		}
		tp->pending_work = 0;
		pthread_mutex_unlock(&tp->mutex);
		JOB job;
		while (tp->jobs.pop(&job)) {
			worker->proc(&job);
		}
	}
	worker->destroy();
	// fprintf(stderr, "  %d stopped\n", id);
	return (void*)NULL;
}

template<class W, class JOB>
void WorkerPool<W, JOB>::start(int num_workers) {
	pthread_mutex_lock(&mutex);
	if (started) {
		pthread_mutex_unlock(&mutex);
		return;
	}
	started = true;
	pthread_mutex_unlock(&mutex);
	this->num_workers = num_workers;
	int err;
	pthread_t tid;
	for (int i = 0; i < num_workers; i++) {
		struct run_arg* arg = new run_arg();
		arg->id = i;
		arg->tp = this;

		err = pthread_create(&tid, NULL, &WorkerPool::_run_worker, arg);
		if (err != 0) {
			fprintf(stderr, "can't create thread: %s\n", strerror(err));
		}
		else {
			tids.push_back(tid);
		}
	}
}

template<class W, class JOB>
void WorkerPool<W, JOB>::stop() {
	// notify
	pthread_mutex_lock(&mutex);
	if (!started) {
		pthread_mutex_unlock(&mutex);
		return;
	}
	started = false;
	pthread_mutex_unlock(&mutex);
	futex_wake(&pending_work, INT_MAX);
	// wait
	for (size_t i = 0; i < tids.size(); i++) {
		pthread_join(tids[i], NULL);
	}
}

#endif

