/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BINLOG_H_
#define SSDB_BINLOG_H_

#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "../util/thread.h"
#include "../util/bytes.h"

inline
static TERARKDB_NAMESPACE::Slice slice(const Bytes &b){
	return TERARKDB_NAMESPACE::Slice(b.data(), b.size());
}

class Binlog{
private:
	std::string buf;
	static const unsigned int HEADER_LEN = sizeof(uint64_t) + 2;
public:
	Binlog(){}
	Binlog(uint64_t seq, char type, char cmd, const TERARKDB_NAMESPACE::Slice &key);
		
	int load(const Bytes &s);
	int load(const TERARKDB_NAMESPACE::Slice &s);
	int load(const std::string &s);

	uint64_t seq() const;
	char type() const;
	char cmd() const;
	const Bytes key() const;

	const char* data() const{
		return buf.data();
	}
	int size() const{
		return (int)buf.size();
	}
	const std::string repr() const{
		return this->buf;
	}
	std::string dumps() const;
};

// circular queue
class BinlogQueue{
private:
	TERARKDB_NAMESPACE::DB *db;
	std::vector<TERARKDB_NAMESPACE::ColumnFamilyHandle*> cfHandles;
	uint64_t min_seq_;
	uint64_t last_seq;
	int capacity;
	TERARKDB_NAMESPACE::ReadOptions read_opts;
	TERARKDB_NAMESPACE::WriteOptions write_opts;
	std::vector<TERARKDB_NAMESPACE::WriteBatch*> vec_batch;
	Mutex mutex;
	bool enabled;

	volatile bool thread_quit;
	static void* log_clean_thread_func(void *arg);
	int del(uint64_t seq);
	// [start, end] includesive
	int del_range(uint64_t start, uint64_t end);
	
	void clean_obsolete_binlogs();
	void merge();

public:
	BinlogQueue(TERARKDB_NAMESPACE::DB *db, std::vector<TERARKDB_NAMESPACE::ColumnFamilyHandle*> handles, bool enabled=true, int capacity=20000000, bool wal=true);
	~BinlogQueue();
	void begin();
	void lock();
	void unlock();
	void release();
	TERARKDB_NAMESPACE::Status commit();
	void add_log(char type, char cmd, const TERARKDB_NAMESPACE::Slice &key);
	void add_log(char type, char cmd, const std::string &key){
		if(!enabled){
			return;
		}
		add_log(type, cmd, slice(key));
	}

	// rocksdb put
	void Put(const TERARKDB_NAMESPACE::Slice& key, const TERARKDB_NAMESPACE::Slice& value);
	// rocksdb delete
	void Delete(const TERARKDB_NAMESPACE::Slice& key);
	// rocksdb merge
	void Merge(const TERARKDB_NAMESPACE::Slice& key, const TERARKDB_NAMESPACE::Slice& value);

	int get(uint64_t seq, Binlog *log) const;
	int update(uint64_t seq, char type, char cmd, const std::string &key);

	void flush();

	/** @returns
	 1 : log.seq greater than or equal to seq
	 0 : not found
	 -1: error
	 */
	int find_next(uint64_t seq, Binlog *log) const;
	int find_min(Binlog *log) const;
	int find_last(Binlog *log) const;

	uint64_t min_seq() const{
		return min_seq_;
	}
	uint64_t max_seq() const{
		return last_seq;
	}
	std::string stats() const;
};

class Transaction{
private:
	BinlogQueue *logs;
public:
	Transaction(BinlogQueue *logs){
		this->logs = logs;
		logs->begin();
	}
	~Transaction(){
		logs->release();
	}
};


#endif
