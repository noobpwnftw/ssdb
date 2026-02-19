include ../../build_config.mk


all: SSDB_client.h SSDB_impl.h SSDB_impl.cpp
	${CXX} -I../ ${CFLAGS} -c SSDB_impl.cpp
	${AR} -cr libssdb-client.a\
		SSDB_impl.o\
		../util/bytes.o\
		../net/link.o ../net/link_addr.o
	cp SSDB_client.h libssdb-client.a ../../api/cpp

test: all
	${CXX} -o demo demo.cpp libssdb-client.a
	${CXX} -o hello-ssdb hello-ssdb.cpp libssdb-client.a

clean:
	rm -f demo hello-ssdb *.a *.o
