/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_LINK_ADDR_H_
#define NET_LINK_ADDR_H_

#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/un.h>

struct LinkAddr
{
	bool ipv4;
	short family;
	socklen_t addrlen;

	LinkAddr(short in_family, bool is_ipv4);
	LinkAddr(short in_family, const char *ip, int port);

	unsigned short port(){
		return (family != AF_UNIX) ? (ipv4 ? ntohs(addr4.sin_port) : ntohs(addr6.sin6_port)) : 0;
	}
	struct sockaddr* addr(){
		return (family != AF_UNIX) ? (ipv4 ? (struct sockaddr *)&addr4 : (struct sockaddr *)&addr6) : (struct sockaddr *)&addr_un;
	}
	void* sin_addr(){
		return (family != AF_UNIX) ? (ipv4 ? (void*)&addr4.sin_addr : (void*)&addr6.sin6_addr) : (void*)&addr_un.sun_path;
	}

private:
	union {
		struct sockaddr_in addr4;
		struct sockaddr_in6 addr6;
		struct sockaddr_un addr_un;
	};

	LinkAddr(){};
};

#endif
