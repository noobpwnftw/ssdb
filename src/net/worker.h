/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_WORKER_H_
#define NET_WORKER_H_

#include <string>
#include "../util/thread.h"
#include "proc.h"
#include <tbb/queuing_rw_mutex.h>

struct ProcJob{
	int result;
	NetworkServer *serv;
	int fd;
	uint64_t gen;
	Command *cmd;

	const Request *req;
	Response resp;
	Queue<ProcJob, (1 << 16)>* wq;
	int efd;
	void enqueue_write() {
		wq->push(std::move(*this));
		uint64_t one = 1; ::write(efd, &one, sizeof(one));
	}
};

typedef Queue<ProcJob, (1 << 16)> WriteQueue;

class ProcWorker : public WorkerPool<ProcWorker, ProcJob>::Worker{
public:
	ProcWorker(const std::string &name);
	~ProcWorker(){}
	void init();
	void proc(ProcJob* job);
private:
	tbb::queuing_rw_mutex::scoped_lock m_lock;
};

typedef WorkerPool<ProcWorker, ProcJob> ProcWorkerPool;

#endif
