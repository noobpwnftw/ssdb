/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include <vector>
#include "../util/bytes.h"
#include "rocksdb/terark_namespace.h"

using StrPair = std::pair<std::string, std::string>;

namespace TERARKDB_NAMESPACE {
	class Iterator;
}

class Iterator{
public:
	enum Direction{
		FORWARD, BACKWARD
	};
	Iterator(TERARKDB_NAMESPACE::Iterator *it,
			const std::string &end,
			uint64_t limit,
			Direction direction=Iterator::FORWARD);
	~Iterator();
	bool skip(uint64_t offset);
	bool next();
	Bytes key();
	Bytes val();
private:
	TERARKDB_NAMESPACE::Iterator *it;
	std::string end;
	uint64_t limit;
	bool is_first;
	int direction;
};


class KIterator{
public:
	std::string key;
	std::string val;

	KIterator(Iterator *it);
	~KIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


class HIterator{
public:
	std::string key;
	std::string val;

	HIterator(const Bytes &values,
			const std::string &start,
			const std::string &end,
			uint64_t limit,
			Iterator::Direction direction=Iterator::FORWARD);
	bool next();
private:
	size_t index;
	uint64_t limit;
	std::vector<StrPair> values;
};


class ZIterator{
public:
	std::string name;
	std::string key;
	std::string score;

	ZIterator(Iterator *it, const Bytes &name);
	~ZIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};


#endif
