/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_HASH_H_
#define SSDB_HASH_H_

#include "ssdb_impl.h"

static const std::string kDelTag = "32767";

inline static
std::string encode_hsize_key(const Bytes &name){
	std::string buf;
	buf.reserve(name.size() + 1);
	buf.append(1, DataType::HSIZE);
	buf.append(name.data(), name.size());
	return buf;
}

inline static
int decode_hsize_key(const Bytes &slice, std::string *name){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(name) == -1){
		return -1;
	}
	return 0;
}
const char SQ_File[90] = {
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
};
const char SQ_Rank[90] = {
	'0', '0', '0', '0', '0', '0', '0', '0', '0',
	'1', '1', '1', '1', '1', '1', '1', '1', '1',
	'2', '2', '2', '2', '2', '2', '2', '2', '2',
	'3', '3', '3', '3', '3', '3', '3', '3', '3',
	'4', '4', '4', '4', '4', '4', '4', '4', '4',
	'5', '5', '5', '5', '5', '5', '5', '5', '5',
	'6', '6', '6', '6', '6', '6', '6', '6', '6',
	'7', '7', '7', '7', '7', '7', '7', '7', '7',
	'8', '8', '8', '8', '8', '8', '8', '8', '8',
	'9', '9', '9', '9', '9', '9', '9', '9', '9',
};

inline static
std::string encode_hash_name(const Bytes &name){
	std::string buf;
	buf.reserve(1 + name.size());
	buf.append(1, DataType::HASH);
	buf.append(name.data(), name.size());
	return buf;
}

inline static
int decode_hash_name(const Bytes &slice, std::string *name){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(name) == -1){
		return -1;
	}
	return 0;
}

inline static
std::string encode_hash_value(const Bytes &key, const Bytes& value){
	std::string buf;
	if(key.size() == 4 || key.size() == 5)
	{
		const char* keydata = key.data();
		char src_file = keydata[0] - 'a';
		char src_rank = keydata[1] - '0';
		char dst_file = keydata[2] - 'a';
		char dst_rank = keydata[3] - '0';
		int16_t promotion = 0;
		if(key.size() == 5)
		{
			switch(keydata[4])
			{
				case 'q':
					dst_rank = 0;
					break;
				case 'r':
					dst_rank = 1;
					break;
				case 'b':
					dst_rank = 2;
					break;
				case 'n':
					dst_rank = 3;
					break;
				default:
					assert(0);
					break;
			}
			promotion = 0x80;
		}
		int16_t encoded = (((src_rank << 3) + src_rank + src_file) << 8) + (dst_rank << 3) + dst_rank + dst_file + promotion;
		buf.append((char*)&encoded, sizeof(int16_t));
		int16_t val = value.Int();
		buf.append((char*)&val, sizeof(int16_t));
	}
	return buf;
}
inline static
int decode_hash_value(const Bytes &slice, std::string *key, std::string* value){
	if(slice.size() < 2 * sizeof(int16_t))
	{
		return -1;
	}
	int16_t encoded = *(int16_t*)slice.data();
	int src = encoded >> 8;
	int dst = encoded & 0x7F;
	if(encoded & 0x80)
	{
		key->resize(5);
		(*key)[0] = SQ_File[src];
		(*key)[1] = SQ_Rank[src];
		(*key)[2] = SQ_File[dst];
		if(SQ_Rank[src] == '7')
			(*key)[3] = '8';
		else if(SQ_Rank[src] == '2')
			(*key)[3] = '1';
		else
			return -1;

		switch(SQ_Rank[dst])
		{
			case '0':
				(*key)[4] = 'q';
				break;
			case '1':
				(*key)[4] = 'r';
				break;
			case '2':
				(*key)[4] = 'b';
				break;
			case '3':
				(*key)[4] = 'n';
				break;
			default:
				return -1;
		}
	}
	else
	{
		key->resize(4);
		(*key)[0] = SQ_File[src];
		(*key)[1] = SQ_Rank[src];
		(*key)[2] = SQ_File[dst];
		(*key)[3] = SQ_Rank[dst];
	}
	int16_t val = *(int16_t*)(slice.data() + sizeof(int16_t));
	*value = std::to_string(val);
	return 0;
}

inline static
int get_hash_value(const Bytes& slice, const Bytes& field, std::string* value) {
	if (slice.empty() || slice.size() % (2 * sizeof(int16_t)) != 0) {
		return 0;
	}
	for (int i = 0; i < slice.size(); i += 2 * sizeof(int16_t)) {
		std::string elem_field, elem_value;
		if(decode_hash_value(Bytes(slice.data() + i, 2 * sizeof(int16_t)), &elem_field, &elem_value) == 0
			&& Bytes(elem_field) == Bytes(field)) {
			*value = elem_value;
			return 1;
		}
	}
	return 0;
}

inline static
int get_hash_values(const Bytes& slice, std::deque<StrPair>& values) {
	if (slice.empty() || slice.size() % (2 * sizeof(int16_t)) != 0) {
		return 0;
	}
	for (int i = 0; i < slice.size(); i += 2 * sizeof(int16_t)) {
		std::string elem_field, elem_value;
		if(decode_hash_value(Bytes(slice.data() + i, 2 * sizeof(int16_t)), &elem_field, &elem_value) == 0) {
			values.push_back(std::make_pair(elem_field, elem_value));
		}
	}
	return 0;
}

inline static
int get_hash_value_count(const Bytes& slice) {
	int cnt = 0;
	if (slice.empty() || slice.size() % (2 * sizeof(int16_t)) != 0) {
		return cnt;
	}
	for (int i = 0; i < slice.size(); i += 2 * sizeof(int16_t)) {
		std::string elem_field, elem_value;
		if(decode_hash_value(Bytes(slice.data() + i, 2 * sizeof(int16_t)), &elem_field, &elem_value) == 0) {
			cnt++;
		}
	}
	return cnt;
}

inline static
int get_hash_bytes(const Bytes& slice, std::deque<BytesPair>& values) {
	if (slice.empty() || slice.size() % (2 * sizeof(int16_t)) != 0) {
		return -1;
	}
	for (int i = 0; i < slice.size(); i += 2 * sizeof(int16_t)) {
		values.push_back(std::make_pair(Bytes(slice.data() + i, sizeof(int16_t)), Bytes(slice.data() + i + sizeof(int16_t), sizeof(int16_t))));
	}
	return 0;
}
#endif
