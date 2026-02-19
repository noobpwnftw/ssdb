/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "proc_hash.h"

int proc_migrate_hset(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if (req.size() < 4 || (req.size() - 1) % 3 != 0) {
		resp->push_back("client_error with item count");
	} else {
		int ret = serv->ssdb->migrate_hset(req, 1);
		if (ret == -1) {
			resp->push_back("error");
			return 0;
		}
		resp->reply_int(0, ret);
	}
	return 0;
}

int proc_hexists(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
	const Bytes &key = req[2];
	std::string val;
	int ret = serv->ssdb->hget(name, key, &val);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_hexists(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::vector<std::string> vals;
	int ret = serv->ssdb->multi_hget(req[1], req, 2, vals);
	if(ret == -1){
		resp->push_back("error");
		return 0;
	}
	resp->push_back("ok");
	for(int i = 0; i < vals.size(); i++){
		resp->push_back(req[2 + i].String());
		resp->push_back(vals[i].empty() ? "0" : "1");
	}
	return 0;
}

int proc_multi_hsize(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
		const Bytes &key = *it;
		int64_t ret = serv->ssdb->hsize(key);
		resp->push_back(key.String());
		if(ret == -1){
			resp->push_back("-1");
		}else{
			resp->add(ret);
		}
	}
	return 0;
}

int proc_multi_hset(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 4 || req.size() % 2 != 0){
		resp->push_back("client_error");
	}else{
		const Bytes &name = req[1];
		int ret = serv->ssdb->multi_hset(name, req, 2);
		resp->reply_int(0, ret);
	}
	return 0;
}

int proc_multi_hdel(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
	int ret = serv->ssdb->multi_hdel(name, req, 2);
	resp->reply_int(0, ret);
	return 0;
}

int proc_multi_hget(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::vector<std::string> vals;
	int ret = serv->ssdb->multi_hget(req[1], req, 2, vals);
	if(ret == -1){
		resp->push_back("error");
		return 0;
	}
	resp->push_back("ok");
	for(int i = 0; i < vals.size(); i++){
		if(!vals[i].empty()){
			resp->push_back(req[2 + i].String());
			resp->push_back(vals[i]);
		}
	}
	return 0;
}

int proc_hsize(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	int64_t ret = serv->ssdb->hsize(req[1]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_hset(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hset(req[1], req[2], req[3]);
	resp->reply_bool(ret);
	return 0;
}

int proc_hget(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string val;
	int ret = serv->ssdb->hget(req[1], req[2], &val);
	resp->reply_get(ret, &val);
	return 0;
}

int proc_hdel(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hdel(req[1], req[2]);
	resp->reply_bool(ret);
	return 0;
}

int proc_hclear(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;
	
	const Bytes &name = req[1];
	int64_t count = serv->ssdb->hclear(name);
	resp->reply_int(0, count);

	return 0;
}

int proc_hgetall(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::vector<StrPair> values;
	int ret = serv->ssdb->hgetall(req[1], values);
	if(ret == -1){
		resp->push_back("error");
		return 0;
	}
	resp->push_back("ok");
	std::vector<StrPair>::iterator iter = values.begin();
	while(iter != values.end())
	{
		resp->push_back(iter->first);
		resp->push_back(iter->second);
		iter++;
	}
	return 0;
}

int proc_hscan(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hscan(req[1], req[2], req[3], limit);
	if(it){
		resp->push_back("ok");
		while(it->next()){
			resp->push_back(it->key);
			resp->push_back(it->val);
		}
		delete it;
	}else{
		resp->push_back("error");
	}
	return 0;
}

int proc_hrscan(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hrscan(req[1], req[2], req[3], limit);
	if(it){
		resp->push_back("ok");
		while(it->next()){
			resp->push_back(it->key);
			resp->push_back(it->val);
		}
		delete it;
	}else{
		resp->push_back("error");
	}
	return 0;
}

int proc_hkeys(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hscan(req[1], req[2], req[3], limit);
	if(it){
		resp->push_back("ok");
		while(it->next()){
			resp->push_back(it->key);
		}
		delete it;
	}else{
		resp->push_back("error");
	}
	return 0;
}

int proc_hvals(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hscan(req[1], req[2], req[3], limit);
	if(it){
		resp->push_back("ok");
		while(it->next()){
			resp->push_back(it->val);
		}
		delete it;
	}else{
		resp->push_back("error");
	}
	return 0;
}

int proc_hlist(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[3].Uint64();
	std::vector<std::string> list;
	int ret = serv->ssdb->hlist(req[1], req[2], limit, &list);
	resp->reply_list(ret, list);
	return 0;
}

int proc_hrlist(NetworkServer *net, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[3].Uint64();
	std::vector<std::string> list;
	int ret = serv->ssdb->hrlist(req[1], req[2], limit, &list);
	resp->reply_list(ret, list);
	return 0;
}

// dir := +1|-1
static int _hincr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(3);

	int64_t by = 1;
	if(req.size() > 3){
		by = req[3].Int64();
	}
	int64_t new_val;
	int ret = ssdb->hincr(req[1], req[2], dir * by, &new_val);
	if(ret == 0){
		resp->reply_status(-1, "value is not an integer or out of range");
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_hincr(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _hincr(serv->ssdb, req, resp, 1);
}

int proc_hdecr(NetworkServer *net, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _hincr(serv->ssdb, req, resp, -1);
}
