/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "version.h"
#include "util/log.h"
#include "util/string_util.h"
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"
#include "proc_sys.h"
#include "proc_kv.h"
#include "proc_hash.h"
#include "proc_zset.h"
#include "proc_queue.h"

#define REG_PROC(c, f)     net->proc_map.set_proc(#c, f, (void*)proc_##c)

void SSDBServer::reg_procs(NetworkServer *net){
	REG_PROC(get, "r");
	REG_PROC(set, "w");
	REG_PROC(del, "w");
	REG_PROC(setx, "w");
	REG_PROC(setnx, "wb");
	REG_PROC(getset, "wb");
	REG_PROC(getbit, "r");
	REG_PROC(setbit, "wb");
	REG_PROC(countbit, "r");
	REG_PROC(substr, "r");
	REG_PROC(getrange, "r");
	REG_PROC(strlen, "r");
	REG_PROC(bitcount, "r");
	REG_PROC(incr, "wb");
	REG_PROC(decr, "wb");
	REG_PROC(scan, "rt");
	REG_PROC(rscan, "rt");
	REG_PROC(keys, "rt");
	REG_PROC(rkeys, "rt");
	REG_PROC(exists, "r");
	REG_PROC(multi_exists, "rt");
	REG_PROC(multi_get, "rt");
	REG_PROC(multi_set, "w");
	REG_PROC(multi_del, "w");
	REG_PROC(ttl, "r");
	REG_PROC(expire, "w");

	REG_PROC(hsize, "r");
	REG_PROC(hget, "r");
	REG_PROC(hset, "w");
	REG_PROC(hdel, "wb");
	REG_PROC(hincr, "wb");
	REG_PROC(hdecr, "wb");
	REG_PROC(hclear, "w");
	REG_PROC(hgetall, "r");
	REG_PROC(hscan, "r");
	REG_PROC(hrscan, "r");
	REG_PROC(hkeys, "r");
	REG_PROC(hvals, "r");
	REG_PROC(hlist, "rt");
	REG_PROC(hrlist, "rt");
	REG_PROC(hexists, "r");
	REG_PROC(multi_hexists, "rt");
	REG_PROC(multi_hsize, "rt");
	REG_PROC(multi_hget, "rt");
	REG_PROC(multi_hset, "w");
	REG_PROC(multi_hdel, "wb");
	REG_PROC(migrate_hset, "w");

	// because zrank may be extremly slow, execute in a seperate thread
	REG_PROC(zrank, "rt");
	REG_PROC(zrrank, "rt");
	REG_PROC(zrange, "rt");
	REG_PROC(zrrange, "rt");
	REG_PROC(redis_zrange, "rt");
	REG_PROC(redis_zrrange, "rt");
	REG_PROC(zsize, "r");
	REG_PROC(zget, "r");
	REG_PROC(zset, "wb");
	REG_PROC(zdel, "wb");
	REG_PROC(zincr, "wb");
	REG_PROC(zdecr, "wb");
	REG_PROC(zclear, "wb");
	REG_PROC(zfix, "wb");
	REG_PROC(zscan, "rt");
	REG_PROC(zrscan, "rt");
	REG_PROC(zkeys, "rt");
	REG_PROC(zlist, "rt");
	REG_PROC(zrlist, "rt");
	REG_PROC(zcount, "rt");
	REG_PROC(zsum, "rt");
	REG_PROC(zavg, "rt");
	REG_PROC(zremrangebyrank, "wb");
	REG_PROC(zremrangebyscore, "wb");
	REG_PROC(zexists, "r");
	REG_PROC(multi_zexists, "rt");
	REG_PROC(multi_zsize, "rt");
	REG_PROC(multi_zget, "rt");
	REG_PROC(multi_zset, "wb");
	REG_PROC(multi_zdel, "wb");
	REG_PROC(zpop_front, "wb");
	REG_PROC(zpop_back, "wb");

	REG_PROC(qsize, "r");
	REG_PROC(qfront, "r");
	REG_PROC(qback, "r");
	REG_PROC(qpush, "wb");
	REG_PROC(qpush_front, "wb");
	REG_PROC(qpush_back, "wb");
	REG_PROC(qpop, "wb");
	REG_PROC(qpop_front, "wb");
	REG_PROC(qpop_back, "wb");
	REG_PROC(qtrim_front, "wb");
	REG_PROC(qtrim_back, "wb");
	REG_PROC(qfix, "wb");
	REG_PROC(qclear, "wb");
	REG_PROC(qlist, "rt");
	REG_PROC(qrlist, "rt");
	REG_PROC(qslice, "rt");
	REG_PROC(qrange, "rt");
	REG_PROC(qget, "r");
	REG_PROC(qset, "wb");

	REG_PROC(clear_binlog, "wb");
	REG_PROC(flushdb, "wb");

	REG_PROC(dump, "p");
	REG_PROC(sync140, "p");
	REG_PROC(info, "r");
	REG_PROC(version, "r");
	REG_PROC(dbsize, "r");
	REG_PROC(compact, "rt");
}


SSDBServer::SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net){
	this->ssdb = (SSDBImpl *)ssdb;
	this->meta = meta;

	net->data = this;
	this->reg_procs(net);

	int sync_speed = conf.get_num("replication.sync_speed");

	backend_dump = new BackendDump(this->ssdb);
	backend_sync = new BackendSync(this->ssdb, sync_speed);
	expiration = new ExpirationHandler(this->ssdb);
	
	{ // slaves
		const Config *repl_conf = conf.get("replication");
		if(repl_conf != NULL){
			std::vector<Config *> children = repl_conf->children;
			for(std::vector<Config *>::iterator it = children.begin(); it != children.end(); it++){
				Config *c = *it;
				if(c->key != "slaveof"){
					continue;
				}
				std::string ip = c->get_str("ip");
				int port = c->get_num("port");
				if(ip == ""){
					ip = c->get_str("host");
				}
				if(ip == "" || port <= 0 || port > 65535){
					continue;
				}
				bool is_mirror = false;
				std::string type = c->get_str("type");
				if(type == "mirror"){
					is_mirror = true;
				}else{
					type = "sync";
					is_mirror = false;
				}
				
				std::string id = c->get_str("id");
				std::string auth = c->get_str("auth");
				int recv_timeout = c->get_num("recv_timeout");
				
				log_info("slaveof: %s:%d, type: %s", ip.c_str(), port, type.c_str());
				this->slaveof(id, ip, port, auth, 0, "", is_mirror, recv_timeout);
			}
		}
	}
}

SSDBServer::~SSDBServer(){
	std::vector<Slave *>::iterator it;
	for(it = slaves.begin(); it != slaves.end(); it++){
		Slave *slave = *it;
		slave->stop();
		delete slave;
	}

	delete backend_dump;
	delete backend_sync;
	delete expiration;

	log_debug("SSDBServer finalized");
}

int SSDBServer::slaveof(const std::string &id, const std::string &host, int port, const std::string &auth, uint64_t last_seq, const std::string &last_key, bool is_mirror, int recv_timeout){
	Slave *slave = new Slave(ssdb, meta, host.c_str(), port, is_mirror);
	if(!id.empty()){
		slave->set_id(id);
	}
	if(recv_timeout > 0){
		slave->recv_timeout = recv_timeout;
	}
	slave->last_seq = last_seq;
	slave->last_key = last_key;
	slave->auth = auth;
	slave->start();
	slaves.push_back(slave);
	return 0;
}
