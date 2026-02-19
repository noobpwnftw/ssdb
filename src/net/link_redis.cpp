/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "link_redis.h"
#include <unordered_map>

enum REPLY{
	REPLY_BULK = 0,
	REPLY_MULTI_BULK,
	REPLY_INT,
	REPLY_STATUS
};

enum STRATEGY{
	STRATEGY_AUTO,
	STRATEGY_PING,
	STRATEGY_MGET,
	STRATEGY_HMGET,
	STRATEGY_HGETALL,
	STRATEGY_HKEYS,
	STRATEGY_HVALS,
	STRATEGY_SETEX,
	STRATEGY_ZRANGE,
	STRATEGY_ZREVRANGE,
	STRATEGY_ZRANGEBYSCORE,
	STRATEGY_ZREVRANGEBYSCORE,
	STRATEGY_ZADD,
	STRATEGY_ZINCRBY,
	STRATEGY_REMRANGEBYRANK,
	STRATEGY_REMRANGEBYSCORE,
	STRATEGY_NULL
};

static bool inited = false;
static std::unordered_map<std::string, RedisRequestDesc> cmd_table;

struct RedisCommand_raw
{
	int strategy;
	const char *redis_cmd;
	const char *ssdb_cmd;
	int reply_type;
};

static RedisCommand_raw cmds_raw[] = {
	{STRATEGY_AUTO, "auth",		"auth",			REPLY_STATUS},
	{STRATEGY_PING, "ping",		"ping",			REPLY_STATUS},
	{STRATEGY_AUTO, "dbsize",	"dbsize",		REPLY_INT},

	{STRATEGY_AUTO, "get",		"get",			REPLY_BULK},
	{STRATEGY_AUTO, "getset",	"getset",		REPLY_BULK},
	{STRATEGY_AUTO, "set",		"set",			REPLY_STATUS},
	{STRATEGY_AUTO, "setnx",	"setnx",		REPLY_INT},
	{STRATEGY_AUTO, "exists",	"exists",		REPLY_INT},
	{STRATEGY_AUTO, "incr",		"incr",			REPLY_INT},
	{STRATEGY_AUTO, "decr",		"decr",			REPLY_INT},
	{STRATEGY_AUTO, "ttl",		"ttl",			REPLY_INT},
	{STRATEGY_AUTO, "expire",	"expire",		REPLY_INT},
	{STRATEGY_AUTO, "getbit",	"getbit",		REPLY_INT},
	{STRATEGY_AUTO, "setbit",	"setbit",		REPLY_INT},
	{STRATEGY_AUTO, "strlen",	"strlen",		REPLY_INT},
	{STRATEGY_AUTO, "bitcount",	"bitcount",		REPLY_INT},
	{STRATEGY_AUTO, "substr",	"getrange",		REPLY_BULK},
	{STRATEGY_AUTO, "getrange",	"getrange",		REPLY_BULK},
	{STRATEGY_AUTO, "keys", 	"keys", 		REPLY_MULTI_BULK},

	{STRATEGY_AUTO, "hset",		"hset",			REPLY_INT},
	{STRATEGY_AUTO, "hget",		"hget",			REPLY_BULK},
	{STRATEGY_AUTO, "hexists",	"hexists",		REPLY_INT},

	{STRATEGY_AUTO, "del",		"multi_del",	REPLY_INT},
	{STRATEGY_AUTO, "mset",		"multi_set",	REPLY_STATUS},
	{STRATEGY_AUTO, "incrby",	"incr",			REPLY_INT},
	{STRATEGY_AUTO, "decrby",	"decr",			REPLY_INT},

	{STRATEGY_AUTO, "hmset",	"multi_hset",	REPLY_STATUS},
	{STRATEGY_AUTO, "hdel",		"multi_hdel",	REPLY_INT},
	{STRATEGY_AUTO, "hmdel",	"multi_hdel",	REPLY_INT},
	{STRATEGY_AUTO, "hlen",		"hsize",		REPLY_INT},
	{STRATEGY_AUTO, "hincrby",	"hincr",		REPLY_INT},

	{STRATEGY_AUTO, "zcard",	"zsize",		REPLY_INT},
	{STRATEGY_AUTO, "zscore",	"zget",			REPLY_BULK},
	{STRATEGY_AUTO, "zrem",		"multi_zdel",	REPLY_INT},
	{STRATEGY_AUTO, "zrank",	"zrank",		REPLY_INT},
	{STRATEGY_AUTO, "zrevrank",	"zrrank",		REPLY_INT},
	{STRATEGY_AUTO, "zcount",	"zcount",		REPLY_INT},
	{STRATEGY_REMRANGEBYRANK, "zremrangebyrank",	"zremrangebyrank",		REPLY_INT},
	{STRATEGY_REMRANGEBYSCORE, "zremrangebyscore",	"zremrangebyscore",		REPLY_INT},
	
	/////////////////////////////////////
	{STRATEGY_MGET, "mget",		"multi_get",	REPLY_MULTI_BULK},
	{STRATEGY_HMGET, "hmget",	"multi_hget",	REPLY_MULTI_BULK},
	
	{STRATEGY_HGETALL,	"hgetall",		"hgetall",		REPLY_MULTI_BULK},
	{STRATEGY_HKEYS,	"hkeys", 		"hkeys", 		REPLY_MULTI_BULK},
	{STRATEGY_HVALS,	"hvals", 		"hvals", 		REPLY_MULTI_BULK},
	{STRATEGY_SETEX,	"setex",		"setx", 		REPLY_STATUS},
	{STRATEGY_ZRANGE,	"zrange",		"zrange",		REPLY_MULTI_BULK},
	{STRATEGY_ZREVRANGE,"zrevrange",	"zrrange",		REPLY_MULTI_BULK},
	{STRATEGY_ZADD,		"zadd",			"multi_zset", 	REPLY_INT},
	{STRATEGY_ZINCRBY,	"zincrby",		"zincr", 		REPLY_BULK},
	{STRATEGY_ZRANGEBYSCORE,	"zrangebyscore",	"zscan",	REPLY_MULTI_BULK},
	{STRATEGY_ZREVRANGEBYSCORE,	"zrevrangebyscore",	"zrscan",	REPLY_MULTI_BULK},

	{STRATEGY_AUTO,		"lpush",		"qpush_front", 		REPLY_INT},
	{STRATEGY_AUTO,		"rpush",		"qpush_back", 		REPLY_INT},
	{STRATEGY_AUTO,		"lpop",			"qpop_front", 		REPLY_BULK},
	{STRATEGY_AUTO,		"rpop",			"qpop_back", 		REPLY_BULK},
	{STRATEGY_AUTO, 	"llen",			"qsize",			REPLY_INT},
	{STRATEGY_AUTO, 	"lsize",		"qsize",			REPLY_INT},
	{STRATEGY_AUTO,		"lindex",		"qget", 			REPLY_BULK},
	{STRATEGY_AUTO,		"lset",			"qset", 			REPLY_STATUS},
	{STRATEGY_AUTO,		"lrange",		"qslice",			REPLY_MULTI_BULK},

	{STRATEGY_AUTO, 	NULL,			NULL,			0}
};

void RedisLink::init(){
	if(!inited){
		inited = true;
		RedisCommand_raw *def = &cmds_raw[0];
		while(def->redis_cmd != NULL){
			RedisRequestDesc desc;
			desc.strategy = def->strategy;
			desc.redis_cmd = def->redis_cmd;
			desc.ssdb_cmd = def->ssdb_cmd;
			desc.reply_type = def->reply_type;
			cmd_table[desc.redis_cmd] = desc;
			def += 1;
		}
	}
}

int RedisLink::convert_req(){
	std::unordered_map<std::string, RedisRequestDesc>::iterator it;
	it = cmd_table.find(cmd);
	if(it == cmd_table.end()){
		this->req_desc = NULL;
		recv_bytes[0] = Bytes(cmd.data(), cmd.size());
		return 0;
	}
	this->req_desc = &(it->second);

	switch(req_desc->strategy){
	default:
		// STRATEGY_AUTO and all passthrough strategies: only the command name
		// changes, args pass through as-is. recv_bytes[1..n] remain valid views
		// into the input buffer for the lifetime of this command (safe because
		// the link is pipeline-serial: FDEVENT_IN is suppressed while a command
		// is in-flight, so input->nice() cannot run until after recv_req()
		// returns and the response is written).
		recv_bytes[0] = Bytes(req_desc->ssdb_cmd.data(), req_desc->ssdb_cmd.size());
		return 0;

	case STRATEGY_HKEYS:
	case STRATEGY_HVALS:
		recv_string.clear();
		recv_string.push_back(req_desc->ssdb_cmd);
		if(recv_bytes.size() == 2){
			recv_string.push_back(recv_bytes[1].String());
			recv_string.push_back("");
			recv_string.push_back("");
			recv_string.push_back("2000000000");
		}
		return 1;

	case STRATEGY_SETEX:
		recv_string.clear();
		recv_string.push_back(req_desc->ssdb_cmd);
		if(recv_bytes.size() == 4){
			recv_string.push_back(recv_bytes[1].String());
			recv_string.push_back(recv_bytes[3].String());
			recv_string.push_back(recv_bytes[2].String());
		}
		return 1;

	case STRATEGY_ZADD:
		recv_string.clear();
		recv_string.push_back(req_desc->ssdb_cmd);
		if(recv_bytes.size() >= 2){
			recv_string.push_back(recv_bytes[1].String());
			for(int i=2; i<=recv_bytes.size()-2; i+=2){
				recv_string.push_back(recv_bytes[i+1].String());
				int64_t score = (int64_t)recv_bytes[i].Double();
				recv_string.push_back(str(score));
			}
		}
		return 1;

	case STRATEGY_ZINCRBY:
		recv_string.clear();
		recv_string.push_back(req_desc->ssdb_cmd);
		if(recv_bytes.size() == 4){
			recv_string.push_back(recv_bytes[1].String());
			recv_string.push_back(recv_bytes[3].String());
			recv_string.push_back(recv_bytes[2].String());
		}
		return 1;

	case STRATEGY_REMRANGEBYRANK:
	case STRATEGY_REMRANGEBYSCORE:
		recv_string.clear();
		recv_string.push_back(req_desc->ssdb_cmd);
		if(recv_bytes.size() >= 4){
			recv_string.push_back(recv_bytes[1].String());
			recv_string.push_back(recv_bytes[2].String());
			recv_string.push_back(recv_bytes[3].String());
		}
		return 1;

	case STRATEGY_ZRANGE:
	case STRATEGY_ZREVRANGE: {
		recv_string.clear();
		std::string cmd = "redis_";
		cmd += req_desc->ssdb_cmd;
		recv_string.push_back(cmd);
		recv_string.push_back(recv_bytes[1].String());
		if(recv_bytes.size() >= 4){
			recv_string.push_back(recv_bytes[2].String());
			recv_string.push_back(recv_bytes[3].String());
		}
		if(recv_bytes.size() >= 5){
			std::string s = recv_bytes[4].String();
			strtolower(&s);
			recv_string.push_back(s);
		}
		return 1;
	}

	case STRATEGY_ZRANGEBYSCORE:
	case STRATEGY_ZREVRANGEBYSCORE: {
		recv_string.clear();
		recv_string.push_back(req_desc->ssdb_cmd);
		std::string name, smin, smax, withscores, offset, count;
		if(recv_bytes.size() >= 4){
			name = recv_bytes[1].String();
			smin = recv_bytes[2].String();
			smax = recv_bytes[3].String();

			bool after_limit = false;
			for(int i=4; i<recv_bytes.size(); i++){
				std::string s = recv_bytes[i].String();
				if(after_limit){
					if(offset.empty()){
						offset = s;
					}else{
						count = s;
						after_limit = false;
					}
				}
				strtolower(&s);
				if(s == "withscores"){
					withscores = s;
				}else if(s == "limit"){
					after_limit = true;
				}
			}
		}
		if(smin.empty() || smax.empty()){
			return 1;
		}

		recv_string.push_back(name);
		recv_string.push_back("");

		if(smin == "-inf" || smin == "+inf"){
			recv_string.push_back("");
		}else{
			if(smin[0] == '('){
				std::string tmp(smin.data() + 1, smin.size() - 1);
				char buf[32];
				if(req_desc->strategy == STRATEGY_ZRANGEBYSCORE){
					snprintf(buf, sizeof(buf), "%d", str_to_int(tmp) + 1);
				}else{
					snprintf(buf, sizeof(buf), "%d", str_to_int(tmp) - 1);
				}
				smin = buf;
			}
			recv_string.push_back(smin);
		}
		if(smax == "-inf" || smax == "+inf"){
			recv_string.push_back("");
		}else{
			if(smax[0] == '('){
				std::string tmp(smax.data() + 1, smax.size() - 1);
				char buf[32];
				if(req_desc->strategy == STRATEGY_ZRANGEBYSCORE){
					snprintf(buf, sizeof(buf), "%d", str_to_int(tmp) - 1);
				}else{
					snprintf(buf, sizeof(buf), "%d", str_to_int(tmp) + 1);
				}
				smax = buf;
			}
			recv_string.push_back(smax);
		}
		recv_string.push_back(offset.empty() ? "0" : offset);
		recv_string.push_back(count.empty()  ? "2000000000" : count);
		recv_string.push_back(withscores);
		return 1;
	}
	}
}

const std::vector<Bytes>* RedisLink::recv_req(Buffer *input){
	int ret = this->parse_req(input);
	if(ret == -1){
		return NULL;
	}
	if(recv_bytes.empty()){
		if(input->space() == 0){
			input->nice();
			if(input->space() == 0){
				if(input->grow() == -1){
					//log_error("fd: %d, unable to resize input buffer!", this->sock);
					return NULL;
				}
				//log_debug("fd: %d, resize input buffer, %s", this->sock, input->stats().c_str());
			}
		}
		return &recv_bytes;
	}

	cmd = recv_bytes[0].String();
	strtolower(&cmd);

	if(this->convert_req()){
		// Complex strategy: args were reordered/transformed into recv_string.
		// Rebuild recv_bytes as owning views into recv_string.
		recv_bytes.clear();
		for(int i=0; i<recv_string.size(); i++){
			std::string *str = &recv_string[i];
			recv_bytes.push_back(Bytes(str->data(), str->size()));
		}
	}
	// else: STRATEGY_AUTO or unknown command — convert_req() swapped recv_bytes[0]
	// in place; recv_bytes[1..n] remain as valid views into the input buffer.

	return &recv_bytes;
}

int RedisLink::send_resp(Buffer *output, const std::vector<std::string> &resp){
	if(resp.empty()){
		return 0;
	}
	if(resp[0] != "ok"){
		if(resp[0] == "error" || resp[0] == "fail" || resp[0] == "client_error"){
			output->append("-ERR ");
			if(resp.size() >= 2){
				output->append(resp[1]);
			}
			output->append("\r\n");
		}else if(resp[0] == "not_found"){
			output->append("$-1\r\n");
		}else if(resp[0] == "noauth"){
			output->append("-NOAUTH ");
			if(resp.size() >= 2){
				output->append(resp[1]);
			}
			output->append("\r\n");
		}else{
			output->append("-ERR server error\r\n");
		}
		return 0;
	}
	
	// not supported command
	if(req_desc == NULL){
		{
			char buf[32];
			snprintf(buf, sizeof(buf), "*%d\r\n", (int)resp.size() - 1);
			output->append(buf);
		}
		for(int i=1; i<resp.size(); i++){
			const std::string &val = resp[i];
			char buf[32];
			snprintf(buf, sizeof(buf), "$%d\r\n", (int)val.size());
			output->append(buf);
			output->append(val.data(), val.size());
			output->append("\r\n");
		}
		return 0;
	}
	
	if(req_desc->strategy == STRATEGY_PING){
		output->append("+PONG\r\n");
		return 0;
	}
	if(req_desc->reply_type == REPLY_STATUS){
		output->append("+OK\r\n");
		return 0;
	}
	if(req_desc->reply_type == REPLY_BULK){
		if(resp.size() >= 2){
			const std::string &val = resp[1];
			char buf[32];
			snprintf(buf, sizeof(buf), "$%d\r\n", (int)val.size());
			output->append(buf);
			output->append(val.data(), val.size());
			output->append("\r\n");
		}else{
			output->append("$0\r\n");
		}
		return 0;
	}
	if(req_desc->reply_type == REPLY_INT){
		if(resp.size() >= 2){
			const std::string &val = resp[1];
			output->append(":");
			output->append(val.data(), val.size());
			output->append("\r\n");
		}else{
			output->append("$0\r\n");
		}
		return 0;
	}
	if(req_desc->strategy == STRATEGY_MGET || req_desc->strategy == STRATEGY_HMGET){
		if(resp.size() % 2 != 1){
			output->append("*0");
			//log_error("bad response for multi_(h)get");
			return 0;
		}
		char buf[32];
		int req_start;
		if(req_desc->strategy == STRATEGY_MGET){
			req_start = 1;
			snprintf(buf, sizeof(buf), "*%d\r\n", (int)recv_bytes.size() - 1);
		}else{
			req_start = 2;
			snprintf(buf, sizeof(buf), "*%d\r\n", (int)recv_bytes.size() - 2);
		}
		output->append(buf);

		std::vector<std::string>::const_iterator resp_it = resp.begin() + 1;

		for(int i = req_start; i < (int)recv_bytes.size(); i++){
			const Bytes &req_key = recv_bytes[i];
			if(resp_it == resp.end()){
				output->append("$-1\r\n");
				continue;
			}
			const std::string &resp_key = *resp_it;
			//log_debug("%s %s", req_key.String().c_str(), resp_key.c_str());
			if(req_key != Bytes(resp_key)){
				output->append("$-1\r\n");
				// loop until we find value to the requested key
				continue;
			}

			const std::string &val = *(resp_it + 1);
			char buf[32];
			snprintf(buf, sizeof(buf), "$%d\r\n", (int)val.size());
			output->append(buf);
			output->append(val.data(), val.size());
			output->append("\r\n");

			resp_it += 2;
		}

		return 0;
	}
	
	if(req_desc->reply_type == REPLY_MULTI_BULK){
		bool withscores = true;
		if(req_desc->strategy == STRATEGY_ZRANGE || req_desc->strategy == STRATEGY_ZREVRANGE){
			if(recv_string.size() < 5 || recv_string[4] != "withscores"){
				withscores = false;
			}
		}
		if(req_desc->strategy == STRATEGY_ZRANGEBYSCORE || req_desc->strategy == STRATEGY_ZREVRANGEBYSCORE){
			if(recv_string[recv_string.size() - 1] != "withscores"){
				withscores = false;
			}
		}
		{
			char buf[32];
			if(withscores){
				snprintf(buf, sizeof(buf), "*%d\r\n", (int)resp.size() - 1);
			}else{
				snprintf(buf, sizeof(buf), "*%d\r\n", ((int)resp.size() - 1)/2);
			}
			output->append(buf);
		}
		for(int i=1; i<resp.size(); i++){
			const std::string &val = resp[i];
			char buf[32];
			snprintf(buf, sizeof(buf), "$%d\r\n", (int)val.size());
			output->append(buf);
			output->append(val.data(), val.size());
			output->append("\r\n");
			if(!withscores){
				i += 1;
			}
		}
		return 0;
	}
	
	output->append("-ERR server error\r\n");
	return 0;
}

int RedisLink::parse_req(Buffer *input){
	recv_bytes.clear();

	int parsed = 0;
	int size = input->size();
	char *ptr = input->data();
	
	// ignore leading empty lines
	while(size > 0 && (ptr[0] == '\n' || ptr[0] == '\r')){
		ptr ++;
		size --;
		parsed ++;
	}
	//dump(ptr, size);
	
	if(ptr[0] != '*'){
		return -1;
	}

	int num_args = 0;	
	while(size > 0){
		char *lf = (char *)memchr(ptr, '\n', size);
		if(lf == NULL){
			break;
		}
		lf += 1;
		size -= (lf - ptr);
		parsed += (lf - ptr);
		
		int len = (int)strtol(ptr + 1, NULL, 10); // ptr + 1: skip '$' or '*'
		if(errno == EINVAL){
			return -1;
		}
		ptr = lf;
		if(len < 0){
			return -1;
		}
		if(num_args == 0){
			if(len <= 0){
				return -1;
			}
			num_args = len;
			continue;
		}
		
		if(len > size - 1){
			break;
		}
		
		recv_bytes.push_back(Bytes(ptr, len));
		
		ptr += len + 1;
		size -= len + 1;
		parsed += len + 1;
		// compatiabl with both CRLF and LF
		if(size > 0 && *ptr == '\n'){
			ptr += 1;
			size -= 1;
			parsed += 1;
		}

		num_args --;
		if(num_args == 0){
			input->decr(parsed);
			return 1;
		}
	}
	
	recv_bytes.clear();
	return 0;
}
