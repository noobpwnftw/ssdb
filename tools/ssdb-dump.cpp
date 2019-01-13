/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "include.h"
#include <sys/types.h>
#include <sys/stat.h>

#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "ssdb/chess_merge.h"
#include <table/terark_zip_table.h>

#include "include.h"
#include "ssdb/const.h"
#include "net/link.h"
#include "util/log.h"
#include "util/file.h"
#include "util/string_util.h"
#include "block-queue.h"

// per Data (with chess data) is about 50+byte,
// kQueueHardLimit = 10M ~ (1.5G RAM)
static const int kQueueHardLimit = 1 * 1000 * 1000;
static const int kBatchSize = 100;

struct Data {
	std::string key;
	std::string value;
	Data() {
		key.clear();
	}
};

struct DumpConfig {
	std::string ip;
	int port;
	bool hasauth;
	std::string auth;
	std::string output_folder;
};

static const int kThreads = 8;
std::vector<std::thread> threads;
BlockQueue<Data> dqueue;

void put_data(TERARKDB_NAMESPACE::DB* db) {
	TERARKDB_NAMESPACE::WriteBatch batch;
	TERARKDB_NAMESPACE::WriteOptions write_opts;
	write_opts.disableWAL = true;
	while (true) {
		Data data = dqueue.pop();
		if (!data.key.empty()) {
			batch.Put(data.key, data.value);
		}
		if (data.key.empty() || batch.Count() >= kBatchSize) { // end signal
			if (batch.Count() == 0) {
				break;
			}
			TERARKDB_NAMESPACE::Status status = db->Write(write_opts, &batch);
			if(!status.ok()){
				fprintf(stderr, "write rocksdb error!\n");
				fprintf(stderr, "ERROR: failed to dump data!\n");
				exit(1);
			}
			if (data.key.empty()) {
				break;
			}
			batch.Clear();
		}
	}
}

template<class T>
static std::string serialize_req(T &req){
	std::string ret;
	char buf[50];
	for(int i=0; i<req.size(); i++){
		if(i >= 5 && i < req.size() - 1){
			sprintf(buf, "[%d more...]", (int)req.size() - i - 1);
			ret.append(buf);
			break;
		}
		if(((req[0] == "get" || req[0] == "set") && i == 1) || req[i].size() < 30){
			std::string h = hexmem(req[i].data(), req[i].size());
			ret.append(h);
		}else{
			sprintf(buf, "[%d bytes]", (int)req[i].size());
			ret.append(buf);
		}
		if(i < req.size() - 1){
			ret.append(" ");
		}
	}
	return ret;
}

void welcome(){
	printf("ssdb-dump - SSDB backup command\n");
	printf("Copyright (c) 2012-2015 ssdb.io\n");
	printf("\n");
}

void usage(int argc, char **argv){
	printf("Usage:\n"
		"\n"
		"    %s -o output_folder\n"
		"    %s ip port output_folder\n"
		"\n"
		"Options:\n"
		"    -h <ip/hostname>   Server IP address/hostname (default: 127.0.0.1).\n"
		"    -p <port>          Server port (default: 8888).\n"
		"    -a <password>      Password to use when connecting to the server.\n"
		"    -o <output_folder> local backup folder that will be created.\n"
		"\n",
		argv[0], argv[0]);
	exit(1);   
}

int parse_options(DumpConfig *config, int argc, char **argv){
	int i;
	for(i = 1; i < argc; i++) {
		bool lastarg = i==argc-1;
		if(!strcmp(argv[i],"-h") && !lastarg){
			config->ip = argv[++i];
		}else if(!strcmp(argv[i], "-h") && lastarg){
			usage(argc, argv);
		}else if(!strcmp(argv[i], "-p") && !lastarg){
			config->port = atoi(argv[++i]);
		}else if(!strcmp(argv[i], "-a") && !lastarg){
			config->hasauth = true;
			config->auth = argv[++i];
		}else if(!strcmp(argv[i], "-o") && !lastarg){
			config->output_folder = argv[++i];
		}else{
			if(argv[i][0] == '-'){
				fprintf(stderr,
					"Unrecognized option or bad number of args for: '%s'\n",
					argv[i]);
					exit(1);
			}else{
				/* Likely the command name, stop here. */
				break;
			}
		}
	}
	return i;
}

int main(int argc, char **argv){
	welcome();
	set_log_level(Logger::LEVEL_MIN);

	DumpConfig config;
	config.ip = "127.0.0.1";
	config.port = 8888;
	config.hasauth = false;
    
	int firstarg = parse_options(&config, argc, argv);
	if(firstarg == 1 && firstarg + 3 <= argc){
		// compatibale with old style arguments
		config.ip = argv[firstarg + 0];
		config.port = atoi(argv[firstarg + 1]);
		config.output_folder = argv[firstarg + 2];
	}

	if(config.output_folder.empty()){
		fprintf(stderr, "ERROR: -o <output_folder> is required!\n");
		usage(argc, argv);
		exit(1);
	}
    
	if(file_exists(config.output_folder.c_str())){
		fprintf(stderr, "ERROR: output_folder[%s] exists!\n", config.output_folder.c_str());
		exit(1);
	}
	if(mkdir(config.output_folder.c_str(), 0777) == -1){
		fprintf(stderr, "ERROR: error create backup directory!\n");
		exit(1);
	}

	std::string data_dir = "";
	data_dir.append(config.output_folder);
	data_dir.append("/data");
	
	// connect to server
	Link *link = Link::connect(config.ip.c_str(), config.port);
	if(link == NULL){
		fprintf(stderr, "ERROR: error connecting to server: %s:%d!\n", config.ip.c_str(), config.port);
		exit(1);
	}
	if(config.hasauth){
		const std::vector<Bytes> *resp = link->request("auth", config.auth.c_str());
		if(resp == NULL || resp->at(0) != "ok"){
			fprintf(stderr, "ERROR: auth error!\n");
			exit(1);
		}
	}
	link->send("dump", "A", "", "-1");
	link->flush();

	TERARKDB_NAMESPACE::DB* db;
	TERARKDB_NAMESPACE::Options options;
	TERARKDB_NAMESPACE::Status status;
	options.create_if_missing = true;
	options.IncreaseParallelism();
	options.OptimizeUniversalStyleCompaction(1024ULL * 1024 * 1024 * 4);
	options.target_file_size_base = 1024ULL * 1024 * 512;
	options.merge_operator.reset(new ChessMergeOperator());
	options.compression = TERARKDB_NAMESPACE::kLZ4Compression;
	options.compression_opts.max_dict_bytes = 1024ULL * 64;
	options.compression_opts.zstd_max_train_bytes = 1024ULL * 256;
	options.bottommost_compression = TERARKDB_NAMESPACE::kZSTD;
	options.memtable_factory.reset(TERARKDB_NAMESPACE::NewPatriciaTrieRepFactory());
	options.stats_dump_period_sec = 0;
	options.delete_obsolete_files_period_micros = 0;
	options.max_manifest_file_size = 0;
	options.blob_size = -1;
	options.compaction_options_universal.allow_trivial_move = true;
	options.compaction_options_universal.size_ratio = 100;

	options.level0_file_num_compaction_trigger = -1;
	options.level0_slowdown_writes_trigger = -1;
	options.level0_stop_writes_trigger = (1<<30);
	options.soft_pending_compaction_bytes_limit = 0;
	options.hard_pending_compaction_bytes_limit = 0;
	options.disable_auto_compactions = true;
	options.max_compaction_bytes = (static_cast<uint64_t>(1) << 60);

	status = TERARKDB_NAMESPACE::DB::Open(options, data_dir.c_str(), &db);
	if(!status.ok()){
		fprintf(stderr, "ERROR: open rocksdb: %s error!\n", config.output_folder.c_str());
		exit(1);
	}

	// Launch a group of threads
	for (int i = 0; i < kThreads; ++i) {
		threads.push_back(std::thread(put_data, db));
	}

	int64_t dump_count = 0;
	while(1){
		const std::vector<Bytes> *req = link->recv();
		if(req == NULL){
			fprintf(stderr, "recv error\n");
			fprintf(stderr, "ERROR: failed to dump data!\n");
			exit(1);
		}else if(req->empty()){
			int len = link->read();
			if(len <= 0){
				fprintf(stderr, "read error: %s\n", strerror(errno));
				fprintf(stderr, "ERROR: failed to dump data!\n");
				exit(1);
			}
		}else{
			Bytes cmd = req->at(0);
			if(cmd == "begin"){
				printf("recv begin...\n");
			}else if(cmd == "end"){
				printf("received %" PRId64 " entry(s)\n", dump_count);
				printf("recv end\n\n");
				break;
			}else if(cmd == "set"){
				/*
				std::string s = serialize_req(*req);
				printf("%s\n", s.c_str());
				*/

				if(req->size() != 3){
					fprintf(stderr, "invalid set params!\n");
					fprintf(stderr, "ERROR: failed to dump data!\n");
					exit(1);
				}
				Bytes key = req->at(1);
				Bytes val = req->at(2);
				if(key.size() == 0 || key.data()[0] == DataType::SYNCLOG){
					continue;
				}

				Data data;
				data.key = std::string(key.data(), key.size());
				data.value = std::string(val.data(), val.size());

				if (dqueue.push(data) > kQueueHardLimit) {
					usleep(1000);
				}

				dump_count ++;
				if((int)log10(dump_count - 1) != (int)log10(dump_count) || (dump_count > 0 && dump_count % 100000 == 0)){
					printf("received %" PRId64 " entry(s)\n", dump_count);
				}
			}else{
				fprintf(stderr, "error: unknown command %s\n", std::string(cmd.data(), cmd.size()).c_str());
				fprintf(stderr, "ERROR: failed to dump data!\n");
				exit(1);
			}
		}
	}
	printf("total dumped %" PRId64 " entry(s)\n", dump_count);

	{
		std::string val;
		if(db->GetProperty("rocksdb.stats", &val)){
			printf("%s\n", val.c_str());
		}
	}

	for (int i = 0; i < kThreads; i++) {
		dqueue.push(Data()); // end signal
	}
	// Join the threads with the main thread
	for (int i = 0; i < kThreads; ++i) {
		threads[i].join();
	}

	printf("compacting data...\n");
	db->CompactRange(TERARKDB_NAMESPACE::CompactRangeOptions(), nullptr, nullptr);
	
	{
		std::string val;
		if(db->GetProperty("rocksdb.stats", &val)){
			printf("%s\n", val.c_str());
		}
	}

	printf("backup has been made to folder: %s\n", config.output_folder.c_str());
	
	delete link;
	delete db;
	return 0;
}
