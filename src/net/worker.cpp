/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "worker.h"
#include "link.h"
#include "proc.h"
#include "../util/log.h"
#include "../include.h"

static tbb::queuing_rw_mutex g_proc_mutex;

ProcWorker::ProcWorker(const std::string &name){
	this->name = name;
}

void ProcWorker::init(){
	log_debug("%s %d init", this->name.c_str(), this->id);
}

void ProcWorker::proc(ProcJob* job){
	const Request *req = job->req;
	
	proc_t p = (proc_t)job->cmd->proc;
	if(job->cmd->flags & Command::FLAG_WRITE) {
		if(job->cmd->flags & Command::FLAG_BLOCK) {
			m_lock.acquire(g_proc_mutex);
		} else {
			m_lock.acquire(g_proc_mutex, false);
		}
	}
	job->result = (*p)(job->serv, *req, &job->resp);
	if(job->cmd->flags & Command::FLAG_WRITE) {
		m_lock.release();
	}
	job->enqueue_write();
}
