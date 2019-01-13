/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "server.h"
#include "../util/string_util.h"
#include "../util/file.h"
#include "../util/config.h"
#include "../util/log.h"
#include "../util/ip_filter.h"
#include "link.h"
#include <vector>

Mutex g_ip_filter_mutex;

static DEF_PROC(ping);
static DEF_PROC(info);
static DEF_PROC(auth);
static DEF_PROC(list_allow_ip);
static DEF_PROC(add_allow_ip);
static DEF_PROC(del_allow_ip);
static DEF_PROC(list_deny_ip);
static DEF_PROC(add_deny_ip);
static DEF_PROC(del_deny_ip);

#define READER_THREADS 8;
#define WRITER_THREADS 8;

volatile bool quit = false;

void signal_handler(int sig){
	switch(sig){
		case SIGTERM:
		case SIGINT:{
			quit = true;
			break;
		}
	}
}

NetworkServer::NetworkServer(){
	num_readers = READER_THREADS;
	num_writers = WRITER_THREADS;

	link_count = 0;

	ip_filter = new IpFilter();
	
	readonly = false;

	// add built-in procs, can be overridden
	proc_map.set_proc("ping", "r", proc_ping);
	proc_map.set_proc("info", "r", proc_info);
	proc_map.set_proc("auth", "r", proc_auth);
	proc_map.set_proc("list_allow_ip", "r", proc_list_allow_ip);
	proc_map.set_proc("add_allow_ip",  "r", proc_add_allow_ip);
	proc_map.set_proc("del_allow_ip",  "r", proc_del_allow_ip);
	proc_map.set_proc("list_deny_ip",  "r", proc_list_deny_ip);
	proc_map.set_proc("add_deny_ip",   "r", proc_add_deny_ip);
	proc_map.set_proc("del_deny_ip",   "r", proc_del_deny_ip);

	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);
}
	
NetworkServer::~NetworkServer(){
	delete ip_filter;
}

NetworkServer* NetworkServer::init(const char *conf_file, int num_readers, int num_writers){
	if(!is_file(conf_file)){
		fprintf(stderr, "'%s' is not a file or not exists!\n", conf_file);
		exit(1);
	}

	Config *conf = Config::load(conf_file);
	if(!conf){
		fprintf(stderr, "error loading conf file: '%s'\n", conf_file);
		exit(1);
	}
	{
		std::string conf_dir = real_dirname(conf_file);
		if(chdir(conf_dir.c_str()) == -1){
			fprintf(stderr, "error chdir: %s\n", conf_dir.c_str());
			exit(1);
		}
	}
	NetworkServer* serv = init(*conf, num_readers, num_writers);
	delete conf;
	return serv;
}

NetworkServer* NetworkServer::init(const Config &conf, int num_readers, int num_writers){
	static bool inited = false;
	if(inited){
		return NULL;
	}
	inited = true;
	
	NetworkServer *serv = new NetworkServer();
	if(num_readers >= 0){
		serv->num_readers = num_readers;
	}
	if(num_writers >= 0){
		serv->num_writers = num_writers;
	}

	// server
	{
		serv->ip = conf.get_str("server.ip");
		serv->port = conf.get_num("server.port");
		if(serv->ip == NULL || serv->ip[0] == '\0'){
			serv->ip = "127.0.0.1";
		}
		log_info("server listen on %s:%d", serv->ip, serv->port);
	}

	// init auth
	{
		Config *cc = (Config *)conf.get("server");
		if(cc != NULL){
			std::vector<Config *> *children = &cc->children;
			std::vector<Config *>::iterator it;
			for(it = children->begin(); it != children->end(); it++){
				if((*it)->key == "auth"){
					std::string password = (*it)->str();
					if(password.size() && (password.size() < 32 || password == "very-strong-password")){
						log_fatal("weak password is not allowed!");
						fprintf(stderr, "WARNING! Weak password is not allowed!\n");
						exit(1);
					}
					serv->passwords.insert(password);
				}
			}
		}
		if(serv->passwords.empty()){
			serv->need_auth = false;
			log_info("    auth    : off");
		}else{
			serv->need_auth = true;
			log_info("    auth    : on");
		}
	}

	// init ip_filter
	{
		Config *cc = (Config *)conf.get("server");
		if(cc != NULL){
			std::vector<Config *> *children = &cc->children;
			std::vector<Config *>::iterator it;
			for(it = children->begin(); it != children->end(); it++){
				if((*it)->key == "allow"){
					const char *ip = (*it)->str();
					log_info("    allow   : %s", ip);
					serv->ip_filter->add_allow(ip);
				}
				if((*it)->key == "deny"){
					const char *ip = (*it)->str();
					log_info("    deny    : %s", ip);
					serv->ip_filter->add_deny(ip);
				}
			}
		}
	}
	
	std::string readonly = conf.get_str("server.readonly");
	strtolower(&readonly);
	if(readonly == "yes"){
		serv->readonly = true;
	}else{
		readonly = "no";
		serv->readonly = false;
	}
	log_info("    readonly: %s", readonly.c_str());

	return serv;
}

void *NetworkServer::serve(void *arg){
	Link *serv_link;
	Fdevents *fdes;
	const Fdevents::events_t *events;
	ProcWorkerPool *writer;
	ProcWorkerPool *reader;
	ready_list_t ready_list;
	ready_list_t ready_list_2;
	ready_list_t::iterator it;
	std::vector<ProcJob*> jobs;

	NetworkServer *net = (NetworkServer*)arg;
	// server
	{
		serv_link = Link::listen(net->ip, net->port);
		if(serv_link == NULL){
			log_fatal("error opening server socket! %s", strerror(errno));
			fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
			exit(1);
		}
		// see UNP
		// if client send RST between server's calls of select() and accept(),
		// accept() will block until next connection.
		// so, set server socket nonblock.
		serv_link->noblock();
	}

	writer = new ProcWorkerPool("writer");
	writer->start(net->num_writers);
	reader = new ProcWorkerPool("reader");
	reader->start(net->num_readers);

	fdes = new Fdevents();
	fdes->set(serv_link->fd(), FDEVENT_IN, 0, serv_link);
	fdes->set(reader->fd(), FDEVENT_IN, 0, reader);
	fdes->set(writer->fd(), FDEVENT_IN, 0, writer);

	while(!quit){

		ready_list.swap(ready_list_2);
		ready_list_2.clear();
		
		if(!ready_list.empty()){
			// ready_list not empty, so we should return immediately
			events = fdes->wait(0);
		}else{
			events = fdes->wait(50);
		}
		if(events == NULL){
			log_fatal("events.wait error: %s", strerror(errno));
			break;
		}

		for(int i=0; i<(int)events->size(); i++){
			const Fdevent *fde = events->at(i);
			if(fde->data.ptr == serv_link){
				Link *link = net->accept_link(serv_link);
				if(link){
					int cur = __sync_add_and_fetch(&net->link_count, 1);
					log_debug("new link from %s:%d, fd: %d, links: %d",
						link->remote_ip, link->remote_port, link->fd(), cur);
					fdes->set(link->fd(), FDEVENT_IN, 1, link);
				}else{
					log_debug("accept return NULL");
				}
			}else if(fde->data.ptr == reader || fde->data.ptr == writer){
				ProcWorkerPool *worker = (ProcWorkerPool *)fde->data.ptr;
				if(worker->pop(&jobs) == -1){
					log_fatal("reading result from workers error!");
					exit(0);
				}
				while(!jobs.empty()){
					net->proc_result(fdes, jobs.back(), &ready_list);
					jobs.pop_back();
				}
			}else{
				net->proc_client_event(fdes, fde, &ready_list);
			}
		}

		for(it = ready_list.begin(); it != ready_list.end(); it ++){
			Link *link = *it;
			fdes->del(link->fd());

			if(link->error()){
				__sync_fetch_and_sub(&net->link_count, 1);
				delete link;
				continue;
			}

			const Request *req = link->recv();
			if(req == NULL){
				log_warn("fd: %d, link parse error, delete link", link->fd());
				link->mark_error();
				ready_list_2.push_back(link);
				continue;
			}
			if(req->empty()){
				fdes->set(link->fd(), FDEVENT_IN, 1, link);
				continue;
			}

			ProcJob *job = new ProcJob();
			job->link = link;
			job->req = link->last_recv();
			int result = net->proc(writer, reader, job);
			if(result == PROC_THREAD){
				//
			}else if(result == PROC_BACKEND){
				// link_count does not include backend links
				__sync_fetch_and_sub(&net->link_count, 1);
			}else{
				net->proc_result(fdes, job, &ready_list_2);
			}
		} // end foreach ready link

	}

	writer->stop();
	delete writer;
	reader->stop();
	delete reader;

	delete serv_link;
	delete fdes;

	return NULL;
}

Link* NetworkServer::accept_link(Link *serv_link){
	Link *link = serv_link->accept();
	if(link == NULL){
		log_error("accept failed! %s", strerror(errno));
		return NULL;
	}
	if(!ip_filter->check_pass(link->remote_ip)){
		log_debug("ip_filter deny link from %s:%d", link->remote_ip, link->remote_port);
		delete link;
		return NULL;
	}

	link->nodelay();
	link->noblock();
	return link;
}

int NetworkServer::proc_result(Fdevents *fdes, ProcJob *job, ready_list_t *ready_list){
	Link *link = job->link;
	int result = job->result;

	delete job;
	
	if(result == PROC_ERROR){
		link->mark_error();
		ready_list->push_back(link);
	}else{
		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
			if(link->input->empty()){
				fdes->set(link->fd(), FDEVENT_IN, 1, link);
			}else{
				ready_list->push_back(link);
			}
		}else{
			fdes->clr(link->fd(), FDEVENT_IN);
			fdes->set(link->fd(), FDEVENT_OUT, 1, link);
		}
	}
	return result;
}

/*
event:
	read => ready_list OR close
	write => ready_list
proc_result =>
	done: write & (read OR ready_list)
	async: stop (read & write)
	
1. When writing to a link, it may happen to be in the ready_list,
so we cannot close that link in write process, we could only
just mark it as closed.

2. When reading from a link, it is never in the ready_list, so it
is safe to close it in read process, also safe to put it into
ready_list.

3. Ignore FDEVENT_ERR

A link is in either one of these places:
	1. ready list
	2. async worker queue
	3. fdes
So it safe to delete link when processing ready list and async worker result.
*/
int NetworkServer::proc_client_event(Fdevents *fdes, const Fdevent *fde, ready_list_t *ready_list){
	Link *link = (Link *)fde->data.ptr;
	if(fde->events & FDEVENT_IN){
		int len = link->read();
		//log_debug("fd: %d read: %d", link->fd(), len);
		if(len <= 0){
			log_debug("fd: %d, read: %d, delete link", link->fd(), len);
			link->mark_error();
			ready_list->push_back(link);
			return 0;
		}
		ready_list->push_back(link);
	}else if(fde->events & FDEVENT_OUT){
		int len = link->write();
		//log_debug("fd: %d, write: %d", link->fd(), len);
		if(len <= 0){
			log_debug("fd: %d, write: %d, delete link", link->fd(), len);
			link->mark_error();
			ready_list->push_back(link);
			return 0;
		}

		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
			if(link->input->empty()){
				fdes->set(link->fd(), FDEVENT_IN, 1, link);
			}else{
				ready_list->push_back(link);
			}
		}else{
			fdes->clr(link->fd(), FDEVENT_IN);
			fdes->set(link->fd(), FDEVENT_OUT, 1, link);
		}
	}
	return 0;
}

int NetworkServer::proc(ProcWorkerPool *writer, ProcWorkerPool *reader, ProcJob *job){
	job->serv = this;
	job->result = PROC_OK;

	const Request *req = job->req;

	do{
		// AUTH
		if(need_auth && job->link->auth == false && req->at(0) != "auth"){
			job->resp.push_back("noauth");
			job->resp.push_back("authentication required.");
			break;
		}

		job->cmd = proc_map.get_proc(req->at(0));
		if(!job->cmd){
			job->resp.push_back("client_error");
			job->resp.push_back("Unknown Command: " + req->at(0).String());
			break;
		}

		if(readonly && (job->cmd->flags & Command::FLAG_WRITE)){
			job->resp.push_back("client_error");
			job->resp.push_back("Forbidden Command: " + req->at(0).String());
			break;
		}

		if(job->cmd->flags & Command::FLAG_THREAD){
			if(job->cmd->flags & Command::FLAG_WRITE){
				writer->push(job);
			}else{
				reader->push(job);
			}
			return PROC_THREAD;
		}

		proc_t p = job->cmd->proc;
		job->result = (*p)(this, job->link, *req, &job->resp);
	}while(0);
	
	if(job->link->send(job->resp.resp) == -1){
		job->result = PROC_ERROR;
	}else{
		// try to write socket before it would be added to fdevents
		// socket is NONBLOCK, so it won't block.
		if(job->link->write() < 0){
			job->result = PROC_ERROR;
		}
	}

	return job->result;
}


/* built-in procs */

static int proc_ping(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	return 0;
}

static int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back("ideawu's network server framework");
	resp->push_back("version");
	resp->push_back("1.0");
	resp->push_back("links");
	resp->add(net->link_count);
	return 0;
}

static int proc_auth(NetworkServer *net, Link *link, const Request &req, Response *resp){
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		if(!net->need_auth || net->passwords.count(req[1].String()) != 0){
			link->auth = true;
			resp->push_back("ok");
			resp->push_back("1");
		}else{
			resp->push_back("error");
			resp->push_back("invalid password");
		}
	}
	return 0;
}

#define ENSURE_LOCALHOST() do{ \
		if(strcmp(link->remote_ip, "127.0.0.1") != 0 \
			&& strcmp(link->remote_ip, "::1") != 0) \
		{ \
			resp->push_back("noauth"); \
			resp->push_back("this command is only available from localhost"); \
			return 0; \
		} \
	}while(0)

static int proc_list_allow_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();

	resp->push_back("ok");
	Locking l(&g_ip_filter_mutex);
	IpFilter *ip_filter = net->ip_filter;
	if(ip_filter->allow_all){
		resp->push_back("all");
	}
	std::set<std::string>::const_iterator it;
	for(it=ip_filter->allow.begin(); it!=ip_filter->allow.end(); it++){
		std::string ip = *it;
		ip = ip.substr(0, ip.size() - 1);
		resp->push_back(ip);
	}

	return 0;
}

static int proc_add_allow_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		Locking l(&g_ip_filter_mutex);
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->add_allow(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

static int proc_del_allow_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		Locking l(&g_ip_filter_mutex);
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->del_allow(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

static int proc_list_deny_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();

	resp->push_back("ok");
	Locking l(&g_ip_filter_mutex);
	IpFilter *ip_filter = net->ip_filter;
	if(!ip_filter->allow_all){
		resp->push_back("all");
	}
	std::set<std::string>::const_iterator it;
	for(it=ip_filter->deny.begin(); it!=ip_filter->deny.end(); it++){
		std::string ip = *it;
		ip = ip.substr(0, ip.size() - 1);
		resp->push_back(ip);
	}

	return 0;
}

static int proc_add_deny_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		Locking l(&g_ip_filter_mutex);
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->add_deny(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

static int proc_del_deny_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		Locking l(&g_ip_filter_mutex);
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->del_deny(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

