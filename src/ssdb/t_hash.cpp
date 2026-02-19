/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_hash.h"
#include "rocksdb/utilities/write_batch_with_index.h"

static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type);

int SSDBImpl::multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset, char log_type){
	if(name.empty()){
		log_error("empty name!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}

	std::string hkey = encode_hash_name(name);
	std::vector<Bytes>::const_iterator it;
	std::string new_value;
	new_value.reserve((kvs.size() - offset) * 2);
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		if(key.empty()){
			log_error("empty key!");
			return -1;
		}
		if(key.size() > SSDB_KEY_LEN_MAX){
			log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
			return -1;
		}
		const Bytes &val = *(it + 1);
		new_value.append(encode_hash_value(key, val));
	}
	if (!new_value.empty()) {
		Transaction trans(binlogs);
		binlogs->Merge(hkey, slice(new_value));
		binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		TERARKDB_NAMESPACE::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("multi_hset error: %s", s.ToString().c_str());
			return -1;
		}
		return new_value.size() / 4;
	}
	return 0;
}

int SSDBImpl::multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset, char log_type){
	if(name.empty()){
		log_error("empty name!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}

	std::string hkey = encode_hash_name(name);
	std::vector<Bytes>::const_iterator it;
	std::string new_value;
	new_value.reserve((keys.size() - offset) * 4);
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
		if(key.empty()){
			log_error("empty key!");
			return -1;
		}
		if(key.size() > SSDB_KEY_LEN_MAX){
			log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
			return -1;
		}
		new_value.append(encode_hash_value(key, kDelTag));
	}
	if(!new_value.empty()) {
		std::string current;
		TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, hkey, &current);
		if(s.IsNotFound()){
			return 0;
		}else if(!s.ok()){
			return -1;
		}
		int old_size = current.size();
		TERARKDB_NAMESPACE::WriteBatchWithIndex wbwi;
		wbwi.Put(hkey, slice(current));
		wbwi.Merge(hkey, slice(new_value));
		s = wbwi.GetFromBatch(cfHandles[kDefaultCFHandle], options, hkey, &current);
		if(!s.ok()){
			return -1;
		}
		Transaction trans(binlogs);
		if(!current.empty()) {
			binlogs->Put(hkey, slice(current));
			binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		}else{
			binlogs->Delete(hkey);
			binlogs->add_log(log_type, BinlogCommand::HDEL, hkey);
		}
		s = binlogs->commit();
		if(!s.ok()){
			log_error("multi_hdel error: %s", s.ToString().c_str());
			return -1;
		}
		return (old_size - current.size()) / 4;
	}
	return 0;
}

int SSDBImpl::sync_hset(const Bytes &name, const Bytes &val, char log_type){
	Transaction trans(binlogs);
	binlogs->Put(slice(name), slice(val));
	binlogs->add_log(log_type, BinlogCommand::HSET, slice(name));
	TERARKDB_NAMESPACE::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("sync_hset error: %s", s.ToString().c_str());
		return -1;
	}
	return 0;
}

int SSDBImpl::sync_hdel(const Bytes &name, char log_type){
	Transaction trans(binlogs);
	binlogs->Delete(slice(name));
	binlogs->add_log(log_type, BinlogCommand::HDEL, slice(name));
	TERARKDB_NAMESPACE::Status s = binlogs->commit();
	if (!s.ok()) {
		log_error("sync_hdel error: %s", s.ToString().c_str());
		return -1;
	}
	return 0;
}

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	return hset_one(this, name, key, val, log_type);
}

int SSDBImpl::hdel(const Bytes &name, const Bytes &key, char log_type){
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}

	std::string hkey = encode_hash_name(name);
	std::string new_value = encode_hash_value(key, kDelTag);
	if(!new_value.empty()) {
		std::string current;
		TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, hkey, &current);
		if(s.IsNotFound()){
			return 0;
		}else if(!s.ok()){
			return -1;
		}
		int old_size = current.size();
		TERARKDB_NAMESPACE::WriteBatchWithIndex wbwi;
		wbwi.Put(hkey, slice(current));
		wbwi.Merge(hkey, slice(new_value));
		s = wbwi.GetFromBatch(cfHandles[kDefaultCFHandle], options, hkey, &current);
		if(!s.ok()){
			return -1;
		}
		Transaction trans(binlogs);
		if(!current.empty()) {
			binlogs->Put(hkey, slice(current));
			binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		}else{
			binlogs->Delete(hkey);
			binlogs->add_log(log_type, BinlogCommand::HDEL, hkey);
		}
		s = binlogs->commit();
		if(!s.ok()){
			log_error("hdel error: %s", s.ToString().c_str());
			return -1;
		}
		return (old_size - current.size()) / 4;
	}
	return 0;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	std::string old;
	int ret = this->hget(name, key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
		*new_val = str_to_int64(old) + by;
		if(errno != 0){
			return 0;
		}
	}
	if(hset_one(this, name, key, str(*new_val), log_type) >= 0){
		return 1;
	}
	return -1;
}

int SSDBImpl::migrate_hset(const std::vector<Bytes>& items, int offset, char log_type) {
	Transaction trans(binlogs);
	for (int i = offset; i < items.size(); i += 3) {
		std::string hkey = encode_hash_name(items[i]);
		Buffer new_value(4);
		new_value.append(items[i + 1]);
		new_value.append(items[i + 2]);
		binlogs->Merge(hkey, TERARKDB_NAMESPACE::Slice(new_value.data(), new_value.size()));
		binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
	}
	TERARKDB_NAMESPACE::Status s = binlogs->commit();
	if (!s.ok()) {
		return -1;
	}
	return (items.size() - offset) / 3;
}

int SSDBImpl::multi_hget(const Bytes &name, const std::vector<Bytes> &keys, int offset, std::vector<std::string> &vals){
	std::string dbkey = encode_hash_name(name);
	std::string value;
	TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, dbkey, &value);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		return -1;
	}
	int n = keys.size() - offset;
	vals.resize(n);
	for(int i = 0; i < n; i++){
		get_hash_value(Bytes(value), keys[offset + i], &vals[i]);
	}
	return 0;
}

int64_t SSDBImpl::hsize(const Bytes &name){
	std::string dbkey = encode_hash_name(name);
	std::string val;
	TERARKDB_NAMESPACE::Status s;

	s = ldb->Get(read_opts, dbkey, &val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		return -1;
	}else{
		return get_hash_value_count(val);
	}
}

int64_t SSDBImpl::hclear(const Bytes &name, char log_type){
	std::string hkey = encode_hash_name(name);
	Transaction trans(binlogs);
	binlogs->Delete(hkey);
	binlogs->add_log(log_type, BinlogCommand::HDEL, hkey);
	TERARKDB_NAMESPACE::Status s = binlogs->commit();
	if (!s.ok()) {
		return -1;
	}
	return 0;
}

int SSDBImpl::hget(const Bytes &name, const Bytes &key, std::string *val){
	std::string dbkey = encode_hash_name(name);
	std::string value;
	TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, dbkey, &value);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("%s", s.ToString().c_str());
		return -1;
	}
	return get_hash_value(Bytes(value), key, val);
}
int SSDBImpl::hgetall(const Bytes &name, std::vector<StrPair>& vals){
	std::string dbkey = encode_hash_name(name);
	std::string value;
	TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, dbkey, &value);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("%s", s.ToString().c_str());
		return -1;
	}
	return get_hash_values(Bytes(value), vals);
}


HIterator* SSDBImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	std::string dbkey = encode_hash_name(name);
	std::string value;
	TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, dbkey, &value);
	if(!s.ok()){
		return NULL;
	}
	return new HIterator(Bytes(value), start.String(), end.String(), limit, Iterator::FORWARD);
}

HIterator* SSDBImpl::hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	std::string dbkey = encode_hash_name(name);
	std::string value;
	TERARKDB_NAMESPACE::Status s = ldb->Get(read_opts, dbkey, &value);
	if(!s.ok()){
		return NULL;
	}
	return new HIterator(Bytes(value), start.String(), end.String(), limit, Iterator::BACKWARD);
}

static void get_hlist(Iterator *it, std::vector<std::string> *list){
	while(it->next()){
		Bytes ks = it->key();
		Bytes vs = it->val();
		if(ks.data()[0] != DataType::HASH){
			break;
		}
		std::string n;
		if(decode_hash_name(ks, &n) == -1){
			continue;
		}
		std::vector<StrPair> values;
		if(get_hash_values(vs, values) == -1){
			continue;
		}
		list->push_back(n);
		list->push_back(str(values.size()));
		std::vector<StrPair>::iterator iter = values.begin();
		while(iter != values.end())
		{
			list->push_back(iter->first);
			list->push_back(iter->second);
			iter++;
		}
	}
}

int SSDBImpl::hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	start = encode_hash_name(name_s);
	if(!name_e.empty()){
		end = encode_hash_name(name_e);
	}
	
	Iterator *it = this->iterator(start, end, limit);
	get_hlist(it, list);
	delete it;
	return 0;
}

int SSDBImpl::hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	start = encode_hash_name(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_hash_name(name_e);
	}
	
	Iterator *it = this->rev_iterator(start, end, limit);
	get_hlist(it, list);
	delete it;
	return 0;
}

// returns the number of newly added items
static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	std::string hkey = encode_hash_name(name);
	std::string new_value = encode_hash_value(key, val);
	if (!new_value.empty()) {
		Transaction trans(ssdb->binlogs);
		ssdb->binlogs->Merge(hkey, slice(new_value));
		ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		TERARKDB_NAMESPACE::Status s = ssdb->binlogs->commit();
		if(!s.ok()){
			return -1;
		}
		return 1;
	}
	return 0;
}
