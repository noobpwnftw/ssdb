
#ifndef CHESS_MERGE_H
#define CHESS_MERGE_H

#include <vector>

#include "rocksdb/merge_operator.h"
#include "ssdb.h"
#include "ssdb_impl.h"
#include "t_hash.h"

class ChessMergeOperator : public TERARKDB_NAMESPACE::MergeOperator {
public:
	ChessMergeOperator() {}
	
	virtual ~ChessMergeOperator() { }

	virtual bool FullMergeV2(const MergeOperationInput& merge_in,
		MergeOperationOutput* merge_out) const {
		// keep newest at front() in arr
		std::vector<BytesPair> arr;
		static const int kExtraLen = 4;
		size_t len = 0;
		// items at back() are newer ones, items at begin() will be
		// overwritten by ones at back()
		for (int i = merge_in.operand_list.size() - 1; i >= -1; i--) {
			std::vector<BytesPair> exists;
			if (i >= 0) {
				const auto& item = merge_in.operand_list[i];
				Bytes slice(item.data(), item.size());
				if (get_hash_bytes(slice, exists) == -1) {
					continue;
				}
			}
			else if (merge_in.existing_value && !merge_in.existing_value->empty()) {
				// filter existing value as well
				Bytes slice(merge_in.existing_value->data(), merge_in.existing_value->size());
				if (get_hash_bytes(slice, exists) == -1) {
					continue;
				}
			}
			else {
				break;
			}
			arr.reserve(arr.size() + exists.size());
			for (auto& item : exists) {
				auto it = std::find_if(arr.begin(), arr.end(), [&item](const BytesPair& p) {
					return (p.first == item.first);
				});
				if (it == arr.end()) {
					arr.emplace_back(std::make_pair(item.first, item.second));
					len += kExtraLen;
				}
			}
		}
		merge_out->new_value.clear();
		auto builder = merge_out->new_value.get_builder();
		builder->uninitialized_resize(len);
		size_t pos = 0;
		auto append = [builder, &pos, &len](const void* data, size_t size) {
			if (size == 0) {
				return true;
			}
			size_t new_pos = pos + size;
			if (new_pos > len) {
				return false;
			}
			memcpy(builder->data() + pos, data, size);
			pos = new_pos;
			return true;
		};
		for (auto& item : arr) {
			if (!isDeleted(item.first, item.second)) {
				append(item.first.data(), item.first.size());
				append(item.second.data(), item.second.size());
			}
		}
		builder->uninitialized_resize(pos);
		return true;
	}

	bool isDeleted(const Bytes& field, const Bytes& value) const {
		return (*(int16_t*)value.data() == 0x7FFF);
	}

	virtual bool PartialMerge(const TERARKDB_NAMESPACE::Slice& key, const TERARKDB_NAMESPACE::Slice& left_operand,
		const TERARKDB_NAMESPACE::Slice& right_operand, std::string* new_value,
		Logger* logger) const {
		std::vector<BytesPair> arr;
		static const int kExtraLen = 4;
		int len = 0;
		for (int i = 0; i < 2; i++) {
			std::vector<BytesPair> exists;
			if (i == 0) { // right is newer
				Bytes slice(right_operand.data(), right_operand.size());
				if (get_hash_bytes(slice, exists) == -1) {
					continue;
				}
			}
			else {
				Bytes slice(left_operand.data(), left_operand.size());
				if (get_hash_bytes(slice, exists) == -1) {
					continue;
				}
			}
			arr.reserve(arr.size() + exists.size());
			for (auto& item : exists) {
				auto it = std::find_if(arr.begin(), arr.end(), [&item](const BytesPair& p) {
					return (p.first == item.first);
				});
				if (it == arr.end()) {
					arr.emplace_back(std::make_pair(item.first, item.second));
					len += kExtraLen;
				}
			}
		}
		new_value->clear();
		new_value->reserve(len);
		for (auto& item : arr) {
			new_value->append(item.first.data(), item.first.size());
			new_value->append(item.second.data(), item.second.size());
		}
		return true;
	}

	virtual const char* Name() const {
		return "ChessMergeOperator";
	}

};

#endif
