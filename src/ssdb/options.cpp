/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "options.h"
#include "../util/string_util.h"

#ifdef NDEBUG
	static const int LOG_QUEUE_SIZE  = 20 * 1000 * 1000;
#else
	static const int LOG_QUEUE_SIZE  = 10000;
#endif

Options::Options(){
	Config c;
	load(c);
}

void Options::load(const Config &conf){
	max_open_files = (size_t)conf.get_num("rocksdb.max_open_files");
	write_buffer_size = (size_t)conf.get_num("rocksdb.write_buffer_size");
	sst_size = (size_t)conf.get_num("rocksdb.sst_size");
	std::string compression_str = conf.get_str("rocksdb.compression");
	std::string binlog_str = conf.get_str("replication.binlog");
	binlog_capacity = (size_t)conf.get_num("replication.binlog.capacity");
	std::string wal_str = conf.get_str("rocksdb.wal");

	strtolower(&compression_str);
	if(compression_str != "no"){
		compression = true;
	}else{
		compression = false;
	}
	strtolower(&binlog_str);
	if(binlog_str != "yes"){
		binlog = false;
	}else{
		binlog = true;
	}
	strtolower(&wal_str);
	if(wal_str != "no"){
		wal = true;
	}else{
		wal = false;
	}
	if(binlog_capacity <= 0){
		binlog_capacity = LOG_QUEUE_SIZE;
	}

	if(write_buffer_size <= 0){
		write_buffer_size = 1024;
	}
	if(sst_size <= 0){
		sst_size = 512;
	}
	if(max_open_files <= 0 || max_open_files > 1000){
		max_open_files = -1;
	}
}
