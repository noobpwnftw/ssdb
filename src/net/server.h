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

	int proc(ProcWorkerPool *writer, ProcWorkerPool *reader, ProcJob *job);

	int num_readers;
	int num_writers;
	bool readonly;
	const char *ip;
	int port;
	NetworkServer();

protected:
	void usage(int argc, char **argv);

public:
	IpFilter *ip_filter;
	void *data;
	ProcMap proc_map;
	int link_count;
	bool need_auth;
    std::set<std::string> passwords;

	~NetworkServer();
	
	// could be called only once
	static NetworkServer* init(const char *conf_file, int num_readers=-1, int num_writers=-1);
	static NetworkServer* init(const Config &conf, int num_readers=-1, int num_writers=-1);
	static void *serve(void *arg);
};


#endif
