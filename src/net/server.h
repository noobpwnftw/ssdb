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

class Link;
class Config;
class IpFilter;
class Fdevents;

typedef std::vector<Link *> ready_list_t;

class NetworkServer
{
private:
	Link* accept_link(Link *serv_link);
	int proc_result(Fdevents *fdes, ProcJob *job, ready_list_t *ready_list);
	int proc_client_event(Fdevents *fdes, const Fdevent *fde, ready_list_t *ready_list);
	int proc(ProcWorkerPool *workers, ProcJob *job);

	Link *serv_link;
	ProcWorkerPool *workers;
	bool readonly;
	const char *ip;
	int port;

protected:
	void usage(int argc, char **argv);

public:
	NetworkServer(const Config &conf);
	~NetworkServer();
	static void *serve(void *arg);
	void *data;
	ProcMap proc_map;
	int link_count;
	IpFilter *ip_filter;
	bool need_auth;
    std::set<std::string> passwords;
};


#endif
