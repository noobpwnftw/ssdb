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

ProcWorker::ProcWorker(const std::string &name){
	this->name = name;
}

void ProcWorker::init(){
	log_debug("%s %d init", this->name.c_str(), this->id);
}

void ProcWorker::proc(ProcJob* job){
	const Request *req = job->req;
	proc_t p = (proc_t)job->cmd->proc;
	job->result = (*p)(job->serv, *req, &job->resp);
	job->enqueue_write();
}
