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
if test -z "$AR"; then
	AR=gcc-ar
fi

case "$TARGET_OS" in
    Linux)
        PLATFORM_CLIBS="-pthread -lgcc -lrt -ldl -ltbb -lgomp -lboost_fiber -lboost_context -latomic"
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
echo AR=$AR >> build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "ROCKSDB_PATH=$ROCKSDB_PATH" >> build_config.mk

echo "CFLAGS = -std=c++14 -faligned-new -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O3 -Wno-sign-compare -march=native -fomit-frame-pointer -flto" >> build_config.mk
echo "CFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk
echo "CFLAGS += -I \"$ROCKSDB_PATH/output/include\"" >> build_config.mk
echo "CFLAGS += -I \"$ROCKSDB_PATH/third-party/terark-zip/src\"" >> build_config.mk
echo "CFLAGS += -I \"$ROCKSDB_PATH\"" >> build_config.mk

echo "CLIBS = -flto=auto" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/libterarkdb.a" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/libterark-zip-r.a" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/libjemalloc.a" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/libsnappy.a" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/liblz4.a" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/libz.a" >> build_config.mk
echo "CLIBS += $ROCKSDB_PATH/output/lib/libbz2.a" >> build_config.mk

echo "CLIBS += ${PLATFORM_CLIBS}" >> build_config.mk
