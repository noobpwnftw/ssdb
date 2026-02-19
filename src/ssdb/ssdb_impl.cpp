/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "chess_merge.h"
#include "chess_filter.h"
#include <table/terark_zip_table.h>

#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"

SSDBImpl::SSDBImpl(){
	ldb = NULL;
	binlogs = NULL;
}

SSDBImpl::~SSDBImpl(){
	if(binlogs){
		delete binlogs;
	}
	if(ldb){
		for (int i = 0; i < cfHandles.size(); i++) {
			ldb->DestroyColumnFamilyHandle(cfHandles[i]);
		}
		delete ldb;
	}
}

SSDB* SSDB::open(const Options &opt, const std::string &dir){
	SSDBImpl *ssdb = new SSDBImpl();
	ssdb->options.create_if_missing = true;
	ssdb->options.create_missing_column_families = true;
	ssdb->options.IncreaseParallelism();
	ssdb->options.OptimizeUniversalStyleCompaction(1024ULL * 1024 * 4 * opt.write_buffer_size);
	ssdb->options.max_open_files = opt.max_open_files;
	ssdb->options.target_file_size_base = 1024ULL * 1024 * opt.sst_size;
	ssdb->options.merge_operator.reset(new ChessMergeOperator());
	ssdb->options.compaction_filter_factory.reset(new ChessCompactionFilterFactory());
	ssdb->options.memtable_factory.reset(TERARKDB_NAMESPACE::NewPatriciaTrieRepFactory());
	ssdb->options.enable_pipelined_write = true;
	ssdb->options.stats_dump_period_sec = 0;
	ssdb->options.delete_obsolete_files_period_micros = 0;
	ssdb->options.max_manifest_file_size = 0;
	ssdb->options.blob_size = -1;
	ssdb->options.compaction_options_universal.allow_trivial_move = true;
	ssdb->options.compaction_options_universal.size_ratio = 1000;
	ssdb->options.compaction_options_universal.max_size_amplification_percent = 10;

	if(opt.compression){
		ssdb->options.compression = TERARKDB_NAMESPACE::kLZ4Compression;
		ssdb->options.compression_opts.max_dict_bytes = 1024ULL * 64;
		ssdb->options.compression_opts.zstd_max_train_bytes = 1024ULL * 256;
		ssdb->options.bottommost_compression = TERARKDB_NAMESPACE::kZSTD;
	}else{
		ssdb->options.compression = TERARKDB_NAMESPACE::kNoCompression;
	}
	ssdb->write_opts.disableWAL = !opt.wal;

	static const std::string kOplogCF = "oplogCF";
	TERARKDB_NAMESPACE::ColumnFamilyOptions oplogOptions;
	oplogOptions.OptimizeUniversalStyleCompaction();
	std::vector<TERARKDB_NAMESPACE::ColumnFamilyDescriptor> cfDescriptors = {
		TERARKDB_NAMESPACE::ColumnFamilyDescriptor(TERARKDB_NAMESPACE::kDefaultColumnFamilyName, ssdb->options),
		TERARKDB_NAMESPACE::ColumnFamilyDescriptor(kOplogCF, oplogOptions)
	};
	TERARKDB_NAMESPACE::Status status = TERARKDB_NAMESPACE::DB::Open(ssdb->options, dir, cfDescriptors, &ssdb->cfHandles, &ssdb->ldb);
	if (!status.ok()) {
		log_error("open db failed: %s", status.ToString().c_str());
		goto err;
	}
	ssdb->binlogs = new BinlogQueue(ssdb->ldb, ssdb->cfHandles, opt.binlog, opt.binlog_capacity, opt.wal);

	return ssdb;
err:
	if(ssdb){
		delete ssdb;
	}
	return NULL;
}

int SSDBImpl::flushdb(){
	int ret = 0;
	TERARKDB_NAMESPACE::WriteBatch batch;
	batch.DeleteRange("", "\xff");
	TERARKDB_NAMESPACE::Status s = ldb->Write(write_opts, &batch);
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		ret = -1;
	}
	binlogs->flush();
	return ret;
}

Iterator* SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit){
	return new Iterator(ldb, start, end, limit);
}

Iterator* SSDBImpl::rev_iterator(const std::string &start, const std::string &end, uint64_t limit){
	return new Iterator(ldb, start, end, limit, Iterator::BACKWARD);
}

/* raw operates */

int SSDBImpl::raw_set(const Bytes &key, const Bytes &val){
	TERARKDB_NAMESPACE::Status s = ldb->Put(write_opts, slice(key), slice(val));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_del(const Bytes &key){
	TERARKDB_NAMESPACE::Status s = ldb->Delete(write_opts, slice(key));
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_get(const Bytes &key, TERARKDB_NAMESPACE::LazyBuffer *val){
	TERARKDB_NAMESPACE::ReadOptions opts;
	opts.fill_cache = false;
	TERARKDB_NAMESPACE::Status s = ldb->Get(opts, slice(key), val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

uint64_t SSDBImpl::size(){
	uint64_t sizes[1];
	ldb->GetIntProperty(cfHandles[kDefaultCFHandle], "rocksdb.estimate-num-keys", sizes);
	return sizes[0];
}

std::vector<std::string> SSDBImpl::info(){
	//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "leveldb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "leveldb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	std::vector<std::string> info;
	std::vector<std::string> keys;
	/*
	for(int i=0; i<7; i++){
		char buf[128];
		snprintf(buf, sizeof(buf), "leveldb.num-files-at-level%d", i);
		keys.push_back(buf);
	}
	*/
	keys.push_back("rocksdb.stats");
	//keys.push_back("rocksdb.sstables");

	for(size_t i=0; i<keys.size(); i++){
		std::string key = keys[i];
		std::string val;
		if(ldb->GetProperty(key, &val)){
			info.push_back(key);
			info.push_back(val);
		}
	}

	return info;
}

void SSDBImpl::compact(int flag){
	if(flag == 2){
		auto* factory = static_cast<ChessCompactionFilterFactory*>(
			options.compaction_filter_factory.get());
		factory->prune = true;
		TERARKDB_NAMESPACE::CompactRangeOptions opts;
		opts.exclusive_manual_compaction = false;
		ldb->CompactRange(opts, nullptr, nullptr);
		factory->prune = false;
	}else if(flag == 1){
		TERARKDB_NAMESPACE::CompactRangeOptions opts;
		opts.exclusive_manual_compaction = false;
		ldb->CompactRange(opts, nullptr, nullptr);
	}else{
		for (int i = 0; i < cfHandles.size(); i++) {
			ldb->Flush(TERARKDB_NAMESPACE::FlushOptions(), cfHandles[i]);
		}
	}
}

int SSDBImpl::key_range(std::vector<std::string> *keys){
	TERARKDB_NAMESPACE::ColumnFamilyMetaData cf_meta;
	ldb->GetColumnFamilyMetaData(cfHandles[kDefaultCFHandle], &cf_meta);

	if(!cf_meta.levels.empty()){
		auto &bottom = cf_meta.levels.back();
		if(!bottom.files.empty()){
			keys->push_back(bottom.files.front().smallestkey);
			for(auto &file : bottom.files){
				keys->push_back(file.largestkey);
			}
		}
	}
	return 0;
}
