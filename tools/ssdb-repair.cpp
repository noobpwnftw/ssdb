/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "include.h"

#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/iterator.h"

#include "util/log.h"
#include "util/file.h"
#include "util/string_util.h"
#include "ssdb/chess_merge.h"

#include <table/terark_zip_table.h>

void welcome(){
	printf("ssdb-repair - SSDB repair tool\n");
	printf("Copyright (c) 2013-2015 ssdb.io\n");
	printf("\n");
}

void usage(int argc, char **argv){
	printf("Usage:\n");
	printf("    %s rocksdb_folder\n", argv[0]);
	printf("\n");
}

int main(int argc, char **argv){
	welcome();

	set_log_level(Logger::LEVEL_MIN);

	if(argc <= 1){
		usage(argc, argv);
		return 0;
	}
	std::string rocksdb_folder(argv[1]);

	if(!file_exists(rocksdb_folder.c_str())){
		printf("rocksdb_folder[%s] not exists!\n", rocksdb_folder.c_str());
		return 0;
	}

	TERARKDB_NAMESPACE::Status status;

	std::shared_ptr<TERARKDB_NAMESPACE::Logger> logger;
	status = TERARKDB_NAMESPACE::Env::Default()->NewLogger("repair.log", &logger);
	if(!status.ok()){
		printf("logger error!\n");
		return 0;
	}
	printf("writing repair log into: repair.log\n");

	TERARKDB_NAMESPACE::Options options;
	options.info_log = logger;
	options.merge_operator.reset(new ChessMergeOperator());
	TerarkZipConfigFromEnv(options, options);
	status = TERARKDB_NAMESPACE::RepairDB(rocksdb_folder.c_str(), options);
	if(!status.ok()){
		printf("repair rocksdb: %s error!\n", rocksdb_folder.c_str());
		return 0;
	}

	printf("rocksdb repaired.\n");

	return 0;
}
