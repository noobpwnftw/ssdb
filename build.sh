#!/bin/sh
BASE_DIR=`pwd`
ROCKSDB_PATH="$BASE_DIR/deps/terarkdb"

# dependency check
which autoconf > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
	echo ""
	echo "ERROR! autoconf required! install autoconf first"
	echo ""
	exit 1
fi

if test -z "$TARGET_OS"; then
	TARGET_OS=`uname -s`
fi
if test -z "$MAKE"; then
	MAKE=make
fi
if test -z "$CC"; then
	CC=gcc
fi
if test -z "$CXX"; then
	CXX=g++
fi

case "$TARGET_OS" in
    Linux)
        PLATFORM_CLIBS="-pthread -lgcc -lrt -ldl -ltbb -laio -lgomp -lsnappy -ltcmalloc -llz4 -lzstd -lz -lbz2"
        ;;
    *)
        echo "Unknown platform!" >&2
        exit 1
esac


DIR=`pwd`
cd "$DIR"

rm -f src/version.h
echo "#ifndef SSDB_DEPS_H" >> src/version.h
echo "#ifndef SSDB_VERSION" >> src/version.h
echo "#define SSDB_VERSION \"`cat version`\"" >> src/version.h
echo "#endif" >> src/version.h
echo "#endif" >> src/version.h
echo "#include <stdlib.h>" >> src/version.h

rm -f build_config.mk
echo CC=$CC >> build_config.mk
echo CXX=$CXX >> build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "ROCKSDB_PATH=$ROCKSDB_PATH" >> build_config.mk

echo "CFLAGS = -std=c++17 -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare" >> build_config.mk
echo "CFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk
echo "CFLAGS += -I \"$ROCKSDB_PATH/include\"" >> build_config.mk
echo "CFLAGS += -I \"$ROCKSDB_PATH\"" >> build_config.mk

echo "CLIBS=" >> build_config.mk
echo "CLIBS += -L$ROCKSDB_PATH/output/lib" >> build_config.mk
echo "CLIBS += -lterarkdb" >> build_config.mk
echo "CLIBS += -lterark-zip-r" >> build_config.mk
echo "CLIBS += -lboost_fiber" >> build_config.mk
echo "CLIBS += -lboost_context" >> build_config.mk

echo "CLIBS += ${PLATFORM_CLIBS}" >> build_config.mk

if test -z "$TMPDIR"; then
    TMPDIR=/tmp
fi

g++ -x c++ - -o $TMPDIR/ssdb_build_test.$$ 2>/dev/null <<EOF
	#include <unordered_map>
	int main() {}
EOF
if [ "$?" = 0 ]; then
	echo "CFLAGS += -DNEW_MAC" >> build_config.mk
fi

