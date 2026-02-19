/*
Copyright (c) 2012-2018 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "proc_sys.h"

int proc_flushdb(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(serv->slaves.size() > 0 || serv->backend_sync->stats().size() > 0){
		resp->push_back("error");
		resp->push_back("flushdb is not allowed when replication is in use!");
		return 0;
	}
	serv->ssdb->flushdb();
	resp->push_back("ok");
	return 0;
}

int proc_compact(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	std::string flag = (req.size() > 1) ? req[1].String() : "";
	if(flag == "prune") {
		serv->ssdb->compact(2);
	}else if(flag == "all") {
		serv->ssdb->compact(1);
	}else{
		serv->ssdb->compact(0);
	}
	resp->push_back("ok");
	return 0;
}

int proc_dbsize(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	uint64_t size = serv->ssdb->size();
	resp->push_back("ok");
	resp->push_back(str(size));
	return 0;
}

int proc_version(NetworkServer *net, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back(SSDB_VERSION);
	return 0;
}

int proc_info(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	resp->push_back("ok");
	resp->push_back("ssdb-server");
	resp->push_back("version");
	resp->push_back(SSDB_VERSION);
	{
		int total = 0;
		for (int i = 0; i < SERVE_THREADS; i++)
			total += net->link_count[i];
		resp->push_back("links");
		resp->add(total);
	}

	if(req.size() > 1 && req[1] == "dbsize"){
		uint64_t size = serv->ssdb->size();
		resp->push_back("dbsize");
		resp->push_back(str(size));
	}

	if(req.size() > 1 && req[1] == "binlogs"){
		serv->ssdb->binlogs->lock();
		std::string s = serv->ssdb->binlogs->stats();
		serv->ssdb->binlogs->unlock();
		resp->push_back("binlogs");
		resp->push_back(s);
	}

	if(req.size() > 1 && req[1] == "replication"){
		{
			std::vector<std::string> syncs = serv->backend_sync->stats();
			std::vector<std::string>::iterator it;
			for(it = syncs.begin(); it != syncs.end(); it++){
				std::string s = *it;
				resp->push_back("replication");
				resp->push_back(s);
			}
		}
		{
			std::vector<Slave *>::iterator it;
			for(it = serv->slaves.begin(); it != serv->slaves.end(); it++){
				Slave *slave = *it;
				std::string s = slave->stats();
				resp->push_back("replication");
				resp->push_back(s);
			}
		}
	}

	if(req.size() > 1 && req[1] == "calls"){
		std::string s;
		for(auto it = net->proc_map.begin(); it != net->proc_map.end(); it++){
			Command *cmd = it->second;
			uint64_t n = cmd->calls.load(std::memory_order_relaxed);
			if(n > 0){
				s += "    " + cmd->name + ": " + str(n) + "\n";
			}
		}
		resp->push_back("calls");
		resp->push_back(s);
	}

	if(req.size() > 1 && req[1] == "range"){
		std::vector<std::string> boundaries;
		serv->ssdb->key_range(&boundaries);
		resp->push_back("range");
		for(auto &key : boundaries){
			resp->push_back(key);
		}
	}

	if(req.size() > 1 && req[1] == "rocksdb"){
		std::vector<std::string> info = serv->ssdb->info();
		resp->push_back("rocksdb");
		for(auto &block : info){
			resp->push_back(block);
		}
	}

	return 0;
}

int proc_redis_info(NetworkServer *net, const Request &req, Response *resp){
	std::string s;
	std::string section = (req.size() > 1) ? req[1].String() : "";

	if(section.empty() || section == "server"){
		s += "ssdb_version:" + std::string(SSDB_VERSION) + "\r\n";
		int total = 0;
		for(int i = 0; i < SERVE_THREADS; i++)
			total += net->link_count[i];
		s += "links:" + str(total) + "\r\n";
	} else if(section == "calls"){
		for(auto it = net->proc_map.begin(); it != net->proc_map.end(); it++){
			Command *cmd = it->second;
			uint64_t n = cmd->calls.load(std::memory_order_relaxed);
			if(n > 0){
				s += cmd->name + ":" + str(n) + "\r\n";
			}
		}
	}

	resp->push_back("ok");
	resp->push_back(s);
	return 0;
}

int proc_dump(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_dump->proc(link);
	return PROC_BACKEND;
}

int proc_sync140(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_sync->proc(link);
	return PROC_BACKEND;
}

int proc_clear_binlog(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->ssdb->binlogs->flush();
	resp->push_back("ok");
	return 0;
}
