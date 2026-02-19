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
#include <unordered_map>
#include <tbb/queuing_rw_mutex.h>

Mutex g_ip_filter_mutex;
static tbb::queuing_rw_mutex g_proc_mutex;

static DEF_PROC(ping);
static DEF_LINK_PROC(auth);
static DEF_LINK_PROC(list_allow_ip);
static DEF_LINK_PROC(add_allow_ip);
static DEF_LINK_PROC(del_allow_ip);
static DEF_LINK_PROC(list_deny_ip);
static DEF_LINK_PROC(add_deny_ip);
static DEF_LINK_PROC(del_deny_ip);

#define WORKER_THREADS 16

volatile bool quit = false;

void signal_handler(int sig){
	switch(sig){
		case SIGTERM:
		case SIGINT:
		{
			quit = true;
			break;
		}
	}
}

NetworkServer::NetworkServer(const Config &conf) {
	memset(link_count, 0, sizeof(link_count));
	memset(link_count_unix, 0, sizeof(link_count_unix));
	serve_max = 0;
	serve_accept = 0;

	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	// add built-in procs, can be overridden
	proc_map.set_proc("ping", "r", (void*)proc_ping);
	proc_map.set_proc("auth", "l", (void*)proc_auth);
	proc_map.set_proc("list_allow_ip", "l", (void*)proc_list_allow_ip);
	proc_map.set_proc("add_allow_ip",  "l", (void*)proc_add_allow_ip);
	proc_map.set_proc("del_allow_ip",  "l", (void*)proc_del_allow_ip);
	proc_map.set_proc("list_deny_ip",  "l", (void*)proc_list_deny_ip);
	proc_map.set_proc("add_deny_ip",   "l", (void*)proc_add_deny_ip);
	proc_map.set_proc("del_deny_ip",   "l", (void*)proc_del_deny_ip);

	// server
	{
		ip = conf.get_str("server.ip");
		port = conf.get_num("server.port");
		sock_path = conf.get_str("server.sock_path");
		if(ip == NULL || ip[0] == '\0'){
			ip = "127.0.0.1";
		}
		if(sock_path == NULL || sock_path[0] == '\0'){
			sock_path = "/tmp/ssdb.sock";
		}
		unlink(sock_path);
		log_info("server listen on %s:%d unix://%s", ip, port, sock_path);
		serv_sock = Link::listen(AF_UNIX, sock_path, 0);
		if(serv_sock == NULL){
			log_fatal("error opening server socket! %s", strerror(errno));
			fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
			exit(1);
		}
		serv_sock->noblock();
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
					passwords.insert(password);
				}
			}
		}
		if(passwords.empty()){
			need_auth = false;
			log_info("    auth    : off");
		}else{
			need_auth = true;
			log_info("    auth    : on");
		}
	}

	// init ip_filter
	{
		ip_filter = new IpFilter();
		Config *cc = (Config *)conf.get("server");
		if(cc != NULL){
			std::vector<Config *> *children = &cc->children;
			std::vector<Config *>::iterator it;
			for(it = children->begin(); it != children->end(); it++){
				if((*it)->key == "allow"){
					const char *ip = (*it)->str();
					log_info("    allow   : %s", ip);
					ip_filter->add_allow(ip);
				}
				if((*it)->key == "deny"){
					const char *ip = (*it)->str();
					log_info("    deny    : %s", ip);
					ip_filter->add_deny(ip);
				}
			}
		}
	}

	std::string readonly_str = conf.get_str("server.readonly");
	strtolower(&readonly_str);
	if(readonly_str == "yes"){
		readonly = true;
	}else{
		readonly_str = "no";
		readonly = false;
	}
	log_info("    readonly: %s", readonly_str.c_str());

	RedisLink::init();
	workers = new ProcWorkerPool("workers");
}
	
NetworkServer::~NetworkServer(){
	delete serv_sock;
	unlink(sock_path);
	delete workers;
	delete ip_filter;
}

void *NetworkServer::serve(void *arg){
	Fdevents *fdes;
	const Fdevents::events_t *events;
	ready_list_t ready_list;
	ready_list_t ready_list_2;
	ready_list_t::iterator it;
	WriteQueue* wq = new WriteQueue();
	std::unordered_map<int, Link*> conns;
	int efd = eventfd(0, EFD_NONBLOCK);

	NetworkServer *net = (NetworkServer*)arg;
	Link* serv_link = Link::listen(AF_INET, net->ip, net->port);
	if(serv_link == NULL){
		log_fatal("error opening server socket! %s", strerror(errno));
		fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
		exit(1);
	}
	serv_link->noblock();
	fdes = new Fdevents();
	fdes->set(serv_link->fd(), FDEVENT_IN, 0, serv_link);
	fdes->set(net->serv_sock->fd(), FDEVENT_IN, 0, net->serv_sock);
	fdes->set(efd, FDEVENT_IN, 0, net);
	net->workers->start(WORKER_THREADS);
	int serve_idx = __sync_fetch_and_add(&net->serve_max, 1);
	while(!quit){

		ready_list.swap(ready_list_2);
		ready_list_2.clear();
		
		if(!ready_list.empty()){
			// ready_list not empty, so we should return immediately
			events = fdes->wait(0);
		}else{
			events = fdes->wait(100);
		}
		if(events == NULL){
			log_fatal("events.wait error: %s", strerror(errno));
			break;
		}

		for(int i=0; i<(int)events->size(); i++){
			const Fdevent *fde = events->at(i);
			if(fde->data.ptr == net){
				uint64_t v; while (::read(efd, &v, sizeof(v)) > 0) {}
				ProcJob job;
				while(wq->pop(&job)){
					auto it = conns.find(job.fd);
					if (it == conns.end())
						continue;
					Link* link = it->second;
					if (link->gen() != job.gen)
						continue;
					if(link->send(job.resp.resp) == -1){
						job.result = PROC_ERROR;
					}else{
						// try to write socket before it would be added to fdevents
						// socket is NONBLOCK, so it won't block.
						if(link->write() < 0){
							job.result = PROC_ERROR;
						}
					}
					net->proc_result(fdes, &job, link, &ready_list_2);
					if(!link->error()){
						fdes->set(link->fd(), FDEVENT_IN, 1, link);
					}
				}
			}else if(fde->data.ptr == serv_link){
				Link* link;
				while((link = net->accept_link(serv_link))) {
					conns.emplace(link->fd(), link);
					int cur = __sync_add_and_fetch(&net->link_count[serve_idx % SERVE_THREADS], 1);
					log_debug("new link from %s:%d, fd: %d, links: %d",
						link->remote_ip, link->remote_port, link->fd(), cur);
					fdes->set(link->fd(), FDEVENT_IN, 1, link);
				}
			}else if(fde->data.ptr == net->serv_sock){
				if(net->serve_accept == serve_idx) {
					Link* link = net->accept_link(net->serv_sock);
					if(link) {
						conns.emplace(link->fd(), link);
						int cur = __sync_add_and_fetch(&net->link_count_unix[serve_idx % SERVE_THREADS], 1);
						log_debug("new link from %s:%d, fd: %d, links: %d",
							link->remote_ip, link->remote_port, link->fd(), cur);
						fdes->set(link->fd(), FDEVENT_IN, 1, link);
						int next_serve_accept = (serve_idx + 1) % net->serve_max;
						int min_link_count = net->link_count_unix[(serve_idx + 1) % SERVE_THREADS];
						for(int j = 1; j < net->serve_max; j++) {
							if(net->link_count_unix[(serve_idx + j) % SERVE_THREADS] < min_link_count) {
								min_link_count = net->link_count_unix[(serve_idx + j) % SERVE_THREADS];
								next_serve_accept = (serve_idx + j) % net->serve_max;
							}
						}
						net->serve_accept = next_serve_accept;
					}
				}
			}else{
				net->proc_client_event(fdes, fde, &ready_list);
			}
		}

		for(it = ready_list.begin(); it != ready_list.end(); it ++){
			Link* link = *it;

			if(link->error()){
				if(link->remote_port != 0)
					__sync_sub_and_fetch(&net->link_count[serve_idx % SERVE_THREADS], 1);
				else
					__sync_sub_and_fetch(&net->link_count_unix[serve_idx % SERVE_THREADS], 1);
				fdes->del(link->fd());
				conns.erase(link->fd());
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
				continue;
			}

			ProcJob job;
			job.fd = link->fd();
			job.gen = link->gen();
			job.wq = wq;
			job.efd = efd;
			int result = net->proc(&job, link);
			if(result == PROC_THREAD){
				fdes->clr(link->fd(), FDEVENT_IN);
			}else if(result == PROC_BACKEND){
				// link_count does not include backend links
				if(link->remote_port != 0)
					__sync_sub_and_fetch(&net->link_count[serve_idx % SERVE_THREADS], 1);
				else
					__sync_sub_and_fetch(&net->link_count_unix[serve_idx % SERVE_THREADS], 1);
				fdes->del(link->fd());
				conns.erase(link->fd());
			}else{
				net->proc_result(fdes, &job, link, &ready_list_2);
			}
		} // end foreach ready link
	}
	net->workers->stop();
	while (!conns.empty()) {
		Link* link = conns.begin()->second;
		if(link->remote_port != 0)
			__sync_sub_and_fetch(&net->link_count[serve_idx % SERVE_THREADS], 1);
		else
			__sync_sub_and_fetch(&net->link_count_unix[serve_idx % SERVE_THREADS], 1);
		fdes->del(link->fd());
		conns.erase(link->fd());
		delete link;
	}
	fdes->del(serv_link->fd());
	fdes->del(net->serv_sock->fd());
	delete fdes;
	delete serv_link;
	close(efd);
	delete wq;
	return NULL;
}

Link* NetworkServer::accept_link(Link* serv_link){
	Link* link = serv_link->accept();
	if(link == NULL){
		return NULL;
	}
	if(!ip_filter->check_pass(link->remote_ip)){
		log_debug("ip_filter deny link from %s:%d", link->remote_ip, link->remote_port);
		delete link;
		return NULL;
	}
	link->noblock();
	return link;
}

void NetworkServer::proc_result(Fdevents *fdes, ProcJob *job, Link* link, ready_list_t *ready_list){
	if(job->result == PROC_ERROR){
		link->mark_error();
		ready_list->push_back(link);
	}else{
		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
			if(!link->input->empty()){
				ready_list->push_back(link);
			}
		}else{
			fdes->set(link->fd(), FDEVENT_OUT, 1, link);
		}
	}
}

void NetworkServer::proc_client_event(Fdevents *fdes, const Fdevent *fde, ready_list_t *ready_list){
	Link* link = (Link*)fde->data.ptr;
	if(fde->events & FDEVENT_IN){
		int len = link->read();
		//log_debug("fd: %d read: %d", link->fd(), len);
		if(len <= 0){
			log_debug("fd: %d, read: %d, delete link", link->fd(), len);
			link->mark_error();
			ready_list->push_back(link);
			return;
		}
		ready_list->push_back(link);
	}else if(fde->events & FDEVENT_OUT){
		int len = link->write();
		//log_debug("fd: %d, write: %d", link->fd(), len);
		if(len <= 0){
			log_debug("fd: %d, write: %d, delete link", link->fd(), len);
			link->mark_error();
			ready_list->push_back(link);
			return;
		}

		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
			if(!link->input->empty()){
				ready_list->push_back(link);
			}
		}else{
			fdes->set(link->fd(), FDEVENT_OUT, 1, link);
		}
	}
}

int NetworkServer::proc(ProcJob *job, Link* link){
	job->serv = this;
	job->result = PROC_OK;
	job->req = link->last_recv();

	const Request *req = job->req;

	do{
		// AUTH
		if(need_auth && link->auth == false && req->at(0) != "auth"){
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
		if(job->cmd->flags & Command::FLAG_LINK) {
			proc_link_t p = (proc_link_t)job->cmd->proc;
			job->result = (*p)(this, link, *req, &job->resp);
			break;
		}

		if(readonly && (job->cmd->flags & Command::FLAG_WRITE)){
			job->resp.push_back("client_error");
			job->resp.push_back("Forbidden Command: " + req->at(0).String());
			break;
		}

		if(job->cmd->flags & Command::FLAG_THREAD){
			workers->push(*job);
			return PROC_THREAD;
		}
		{
			tbb::queuing_rw_mutex::scoped_lock m_lock;
			proc_t p = (proc_t)job->cmd->proc;
			if(job->cmd->flags & Command::FLAG_WRITE) {
				if(job->cmd->flags & Command::FLAG_BLOCK) {
					m_lock.acquire(g_proc_mutex);
				} else {
					m_lock.acquire(g_proc_mutex, false);
				}
			}
			job->result = (*p)(this, *req, &job->resp);
			if(job->cmd->flags & Command::FLAG_WRITE) {
				m_lock.release();
			}
		}
	}while(0);
	
	if(link->send(job->resp.resp) == -1){
		job->result = PROC_ERROR;
	}else{
		// try to write socket before it would be added to fdevents
		// socket is NONBLOCK, so it won't block.
		if(link->write() < 0){
			job->result = PROC_ERROR;
		}
	}

	return job->result;
}


/* built-in procs */

static int proc_ping(NetworkServer *net, const Request &req, Response *resp){
	resp->push_back("ok");
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

