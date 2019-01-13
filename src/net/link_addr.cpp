#include <string.h>
#include <stdio.h>
#include "link_addr.h"

LinkAddr::LinkAddr(bool is_ipv4){
	if(is_ipv4){
		ipv4 = true;
		family = AF_INET;
		addrlen = sizeof(addr4);
	}else{
		ipv4 = false;
		family = AF_INET6;
		addrlen = sizeof(addr6);
	}
}

LinkAddr::LinkAddr(const char *ip, int port){
	if(strchr(ip, ':') == NULL){
		ipv4 = true;
		family = AF_INET;
		addrlen = sizeof(addr4);
		bzero(&addr4, sizeof(addr4));
		addr4.sin_family = family;
		addr4.sin_port = htons((short)port);
		inet_pton(family, ip, &addr4.sin_addr);
	}else{
		ipv4 = false;
		family = AF_INET6;
		addrlen = sizeof(addr6);
		bzero(&addr6, sizeof(addr6));
		addr6.sin6_family = family;
		addr6.sin6_port = htons((short)port);
		inet_pton(family, ip, &addr6.sin6_addr);
	}
}
