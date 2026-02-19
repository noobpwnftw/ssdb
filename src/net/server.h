/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_SERVER_H_
#define NET_SERVER_H_

#include "../include.h"
#include <string>
#include <vector>
#include <set>

#include "fde.h"
#include "proc.h"
#include "worker.h"
#include "../util/thread.h"
#include <tbb/queuing_rw_mutex.h>

extern tbb::queuing_rw_mutex g_proc_mutex;

#define SERVE_THREADS 32

class Link;
class Config;
class IpFilter;
class Fdevents;

typedef std::vector<Link *> ready_list_t;

class NetworkServer
{
private:
	Link* accept_link(Link *serv_link);
	void proc_result(Fdevents *fdes, ProcJob *job, Link* link, ready_list_t *ready_list_2);
	void proc_client_event(Fdevents *fdes, const Fdevent *fde, ready_list_t *ready_list);
	int proc(ProcJob *job, Link* link, bool backlogged);

	ProcWorkerPool *workers;
	bool readonly;
	const char *sock_path;

protected:
	void usage(int argc, char **argv);

public:
	NetworkServer(const Config &conf);
	~NetworkServer();
	static void *serve(void *arg);
	void *data;
	ProcMap proc_map;
	int link_count[SERVE_THREADS];
	int link_count_unix[SERVE_THREADS];
	int handoff_efd[SERVE_THREADS];
	Queue<Link*, (1 << 16)> handoff_queue[SERVE_THREADS];
	volatile int serve_max;
	volatile int serve_accept;
	IpFilter *ip_filter;
	bool need_auth;
	std::set<std::string> passwords;
	const char *ip;
	int port;
	Link* serv_sock;
};


#endif
