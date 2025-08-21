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

int ProcWorker::proc(ProcJob* job){
	const Request *req = job->req;
	
	proc_t p = job->cmd->proc;
	if(job->cmd->flags & Command::FLAG_WRITE) {
		if(job->cmd->flags & Command::FLAG_BLOCK) {
			m_lock.acquire(g_proc_mutex);
		} else {
			m_lock.acquire(g_proc_mutex, false);
		}
	}
	job->result = (*p)(job->serv, job->link, *req, &job->resp);
	if(job->cmd->flags & Command::FLAG_WRITE) {
		m_lock.release();
	}
	if(job->link->send(job->resp.resp) == -1){
		job->result = PROC_ERROR;
	}else{
		// try to write socket before it would be added to fdevents
		// socket is NONBLOCK, so it won't block.
		if(job->link->write() < 0){
			job->result = PROC_ERROR;
		}
	}

	return 0;
}
