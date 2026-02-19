/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_PROC_H_
#define NET_PROC_H_

#include <vector>
#include <unordered_map>
#include "resp.h"
#include "../util/bytes.h"

class Link;
class NetworkServer;

#define PROC_OK			0
#define PROC_ERROR		-1
#define PROC_THREAD		1
#define PROC_BACKEND	100

#define DEF_PROC(f) int proc_##f(NetworkServer *net, const Request &req, Response *resp)
#define DEF_LINK_PROC(f) int proc_##f(NetworkServer *net, Link* link, const Request &req, Response *resp)

typedef std::vector<Bytes> Request;
typedef int (*proc_t)(NetworkServer *net, const Request &req, Response *resp);
typedef int (*proc_link_t)(NetworkServer *net, Link* link, const Request &req, Response *resp);

struct Command{
	static const int FLAG_READ		= (1 << 0);
	static const int FLAG_WRITE		= (1 << 1);
	static const int FLAG_BLOCK		= (1 << 2);
	static const int FLAG_THREAD	= (1 << 3);
	static const int FLAG_LINK		= (1 << 4);

	std::string name;
	int flags;
	void* proc;

	Command(){
		flags = 0;
		proc = NULL;
	}
};

struct BytesEqual{
	bool operator()(const Bytes &s1, const Bytes &s2) const {
		return s1 == s2;
	}
};
struct BytesHash{
	size_t operator()(const Bytes &s1) const {
		return std::hash<std::string>{}(s1.String());
	}
};

typedef std::unordered_map<Bytes, Command *, BytesHash, BytesEqual> proc_map_t;

class ProcMap
{
private:
	proc_map_t proc_map;

public:
	ProcMap();
	~ProcMap();
	void set_proc(const std::string &cmd, const char *sflags, void* proc);
	void set_proc(const std::string &cmd, void* proc);
	Command* get_proc(const Bytes &str);

	proc_map_t::iterator begin(){
		return proc_map.begin();
	}
	proc_map_t::iterator end(){
		return proc_map.end();
	}
};

#endif
