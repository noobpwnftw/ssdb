#include <string.h>
#include <stdio.h>
#include "link_addr.h"

LinkAddr::LinkAddr(short in_family, bool is_ipv4){
	if(in_family == AF_UNIX){
		ipv4 = true;
		family = AF_UNIX;
		addrlen = sizeof(addr_un);
	}
	else if(is_ipv4){
		ipv4 = true;
		family = AF_INET;
		addrlen = sizeof(addr4);
	}else{
		ipv4 = false;
		family = AF_INET6;
		addrlen = sizeof(addr6);
	}
}

LinkAddr::LinkAddr(short in_family, const char *ip, int port){
	if(in_family == AF_UNIX){
		ipv4 = true;
		family = AF_UNIX;
		addrlen = sizeof(addr_un);
		bzero(&addr_un, sizeof(addr_un));
		addr_un.sun_family = family;
		strncpy(addr_un.sun_path, ip, sizeof(addr_un.sun_path) - 1);
	}
	else if(strchr(ip, ':') == NULL){
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
