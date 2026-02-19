
#include <time.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <thread>
#include <vector>
#include "rocksdb/db.h"
#include "util/log.h"
#include "util/string_util.h"
#include "../src/client/SSDB_client.h"
#include "../src/net/link.h"
#include "../src/ssdb/const.h"
#include "block-queue.h"

// per Data (with chess data) is about 50+byte,
// kQueueHardLimit = 10M ~ (1.5G RAM)
static const int kQueueHardLimit = 1 * 1000 * 1000;
static const int kBatchSize = 100;

struct Data {
	std::string key;
	std::string field;
	std::string value;
	Data() {
		key.clear();
	}
};

ssdb::Client *src = NULL;
ssdb::Client *dst = NULL;

static const int kThreads = 8;
std::vector<ssdb::Client*> clients;
std::vector<std::thread> threads;
BlockQueue<Data> dqueue;

void welcome() {
	printf("ssdb-migrate - SSDB server migration tool\n");
	printf("Copyright (c) 2012-2015 ssdb.io\n");
	printf("\n");
}

void usage(int argc, char **argv) {
	printf("Usage:\n"
		"    %s src_ip src_port dst_ip dst_port limit\n"
		"\n"
		"Options:\n"
		"    src_ip    IP addr of the SSDB server to move data from, example: 127.0.0.1\n"
		"    src_port  Port number of source SSDB server\n"
		"    src_ip    IP addr of the SSDB server to move data to, example: 127.0.0.1\n"
		"    dst_port  Port number of destination SSDB server\n"
		"    limit     Approximated number of keys to be moved, example: 1000\n"
		"    -h        Show this message"
		"\n"
		"Example:\n"
		"    %s 127.0.0.1 8887 127.0.0.1 8889 13\n"
		"\n",
		argv[0], argv[0]);
	exit(1);
}

struct AppArgs {
	std::string type;
	std::string src_ip;
	int src_port;
	std::string dst_ip;
	int dst_port;
	int limit;
};

void parse_args(AppArgs *args, int argc, char **argv) {
	if (argc < 6) {
		usage(argc, argv);
	}
	for (int i = 1; i < argc; i++) {
		if (std::string("-h") == argv[i]) {
			usage(argc, argv);
		}
		/*if (argv[i][0] == '-') {
		  fprintf(stderr, "ERROR: Invalid argument: %s!\n", argv[i]);
		  exit(1);
		  }*/
	}
	args->src_ip = argv[1];
	args->src_port = str_to_int(argv[2]);
	args->dst_ip = argv[3];
	args->dst_port = str_to_int(argv[4]);
	args->limit = str_to_int(argv[5]);
	/*if(args->limit <= 0){
	  fprintf(stderr, "ERROR: invalid limit option!\n");
	  exit(1);
	  }*/
}

void send_req(int tid) {
	std::vector<std::string> items;
	while (true) {
		Data data = dqueue.pop();
		if (!data.key.empty()) {
			items.push_back(data.key);
			items.push_back(data.field);
			items.push_back(data.value);
		}
		if (data.key.empty() || items.size() >= kBatchSize) { // end signal
			if (items.empty()) {
				break;
			}
			ssdb::Status s = clients[tid]->migrate_hset(items);
			if (!s.ok()) {
				log_error("dst hset error! %s", s.code().c_str());
				printf("dst hset error! %s", s.code().c_str());
				exit(1);
			}
			if (data.key.empty()) {
				break;
			}
			items.clear();
		}
	}
}

bool init_client(const std::string &ip, int port) {
	for (int i = 0; i < kThreads; i++) {
		ssdb::Client *client = ssdb::Client::connect(ip, port);
		if (client == NULL) {
			log_error("fail to connect to server!");
			return false;
		}
		clients.push_back(client);
	}
	// Launch a group of threads
	for (int i = 0; i < kThreads; ++i) {
		threads.push_back(std::thread(send_req, i));
	}
	return true;
}

int main(int argc, char **argv) {
	welcome();
	set_log_level(Logger::LEVEL_MIN);

	AppArgs args;
	parse_args(&args, argc, argv);

	// connect to src server
	Link *link = Link::connect(args.src_ip.c_str(), args.src_port);
	if (link == NULL) {
		fprintf(stderr, "ERROR: error connecting to src server: %s:%d!\n",
			args.src_ip.c_str(), args.src_port);
		exit(1);
	}
	// connect to dst server
	if (!init_client(args.dst_ip, args.dst_port)) {
		log_error("fail to connect to server!");
		return 0;
	}
	printf("after connect\n");
	// start transfer
	link->send("dump", "A", "", "-1");
	link->flush();

	time_t stime;
	time(&stime);
	int64_t dump_count = 0;
	while (1) {
		const std::vector<Bytes> *req = link->recv();
		if (req == NULL) {
			fprintf(stderr, "recv error\n");
			fprintf(stderr, "ERROR: failed to dump data!\n");
			exit(1);
		}
		else if (req->empty()) {
			int len = link->read();
			if (len <= 0) {
				fprintf(stderr, "read error: %s\n", strerror(errno));
				fprintf(stderr, "ERROR: failed to dump data!\n");
				exit(1);
			}
		}
		else {
			Bytes cmd = req->at(0);
			if (cmd == "begin") {
				printf("recv begin...\n");
			}
			else if (cmd == "end") {
				printf("received %" PRId64 " entry(s)\n", dump_count);
				printf("recv end\n\n");
				break;
			}
			else if (cmd == "set") {
				if (req->size() != 3) {
					fprintf(stderr, "invalid set params!\n");
					fprintf(stderr, "ERROR: failed to dump data!\n");
					exit(1);
				}
				Bytes key = req->at(1);
				Bytes val = req->at(2);
				if (key.size() == 0 || key.data()[0] == DataType::SYNCLOG) {
					continue;
				}

				if (key.data()[0] != 'h') { // only support hset right now
					continue;
				}
				int keysize = key.data()[1];
				Bytes hkey(key.data() + 2, key.size() - 2); // skip 'h' & 'key length'
				if (hkey.data()[keysize] != '=') {
					printf("invalid key\n");
					continue;
				}
				Data data;
				data.key = std::string(hkey.data(), keysize);
				data.field = std::string(hkey.data() + keysize + 1, hkey.size() - keysize - 1);
				data.value = std::string(val.data(), val.size());

				if (dqueue.push(data) > kQueueHardLimit) {
					usleep(1000);
				}

				dump_count++;
				if (args.limit != -1 && dump_count >= args.limit) {
					break;
				}
				if (dump_count > 0 && dump_count % 100000 == 0) {
					printf("received %" PRId64 " entry(s)\n", dump_count);
				}
			}
			else {
				fprintf(stderr, "error: unknown command %s\n", std::string(cmd.data(), cmd.size()).c_str());
				fprintf(stderr, "ERROR: failed to dump data!\n");
				exit(1);
			}
		}
	}
	printf("total dumped %" PRId64 " entry(s)\n", dump_count);

	for (int i = 0; i < kThreads; i++) {
		dqueue.push(Data()); // end signal
	}
	// Join the threads with the main thread
	for (int i = 0; i < kThreads; ++i) {
		threads[i].join();
	}

	time_t etime;
	time(&etime);
	printf("stime %ld, etime %ld\n", stime, etime);
	time_t ts = (etime == stime) ? 1 : (etime - stime);
	double speed = (double)dump_count / ts;
	printf("insert: %.2fk, time: %ld s\n", speed / 1000, ts);

	/*
	  {
	  std::string val;
	  if(db->GetProperty("rocksdb.stats", &val)){
	  printf("%s\n", val.c_str());
	  }
	  }

	  printf("compacting data...\n");
	  db->CompactRange(TERARKDB_NAMESPACE::CompactRangeOptions(), nullptr, nullptr);

	  {
	  std::string val;
	  if(db->GetProperty("rocksdb.stats", &val)){
	  printf("%s\n", val.c_str());
	  }
	  }
	*/
	delete link;
	//delete dst;
	return 0;
}
