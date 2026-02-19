PREFIX=/usr/local/ssdb

$(shell sh build.sh 1>&2)
include build_config.mk

all:
	mkdir -p var var_slave
	chmod u+x deps/cpy/cpy
	chmod u+x tools/ssdb-cli
	cd "${ROCKSDB_PATH}"; ./build.sh
	cd src/util; ${MAKE}
	cd src/net; ${MAKE}
	cd src/client; ${MAKE}
	cd src/ssdb; ${MAKE}
	cd src; ${MAKE}
	cd tools; ${MAKE}

install:
	mkdir -p ${PREFIX}
	mkdir -p ${PREFIX}/_cpy_
	mkdir -p ${PREFIX}/deps
	mkdir -p ${PREFIX}/var
	mkdir -p ${PREFIX}/var_slave
	cp -f ssdb-server ssdb.conf ssdb_slave.conf ${PREFIX}
	cp -rf api ${PREFIX}
	cp -rf \
		tools/ssdb-bench \
		tools/ssdb-cli tools/ssdb_cli \
		tools/ssdb-cli.cpy tools/ssdb-dump \
		tools/ssdb-repair \
		${PREFIX}
	cp -rf deps/cpy ${PREFIX}/deps
	chmod 755 ${PREFIX}
	rm -f ${PREFIX}/Makefile

clean:
	rm -f *.exe.stackdump
	rm -rf api/cpy/_cpy_
	rm -f api/python/SSDB.pyc
	rm -rf db_test
	cd deps/cpy; ${MAKE} clean
	cd src/util; ${MAKE} clean
	cd src/ssdb; ${MAKE} clean
	cd src/net; ${MAKE} clean
	cd src; ${MAKE} clean
	cd tools; ${MAKE} clean

clean_all: clean
	cd "${ROCKSDB_PATH}"; rm -rf output
	
