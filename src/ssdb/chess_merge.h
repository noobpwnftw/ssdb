
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

	bool FullMergeV2(const MergeOperationInput& merge_in,
		MergeOperationOutput* merge_out) const override {
		// keep newest at front() in arr
		std::vector<BytesPair> arr;
		static const int kExtraLen = 4;
		size_t len = 0;
		// items at back() are newer ones, items at begin() will be
		// overwritten by ones at back()
		for (int i = merge_in.operand_list.size() - 1; i >= -1; i--) {
			std::vector<BytesPair> exists;
			if (i >= 0) {
				auto& item = merge_in.operand_list[i];
				if (!Fetch(item, &merge_out->new_value)) {
					return false;
				}
				Bytes slice(item.data(), item.size());
				if (get_hash_bytes(slice, exists) == -1) {
					continue;
				}
			}
			else if (merge_in.existing_value) {
				if (!Fetch(*merge_in.existing_value, &merge_out->new_value)) {
					return false;
				}
				if (merge_in.existing_value->empty()) {
					break;
				}
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
					arr.emplace_back(item.first, item.second);
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

	bool PartialMergeMulti(const TERARKDB_NAMESPACE::Slice& /*key*/,
		const std::vector<TERARKDB_NAMESPACE::LazyBuffer>& operand_list,
		TERARKDB_NAMESPACE::LazyBuffer* new_value, TERARKDB_NAMESPACE::Logger* /*logger*/) const override {
		std::vector<BytesPair> arr;
		static const int kExtraLen = 4;
		int len = 0;
		// operand_list is oldest to newest, iterate in reverse so newest wins
		for (int i = operand_list.size() - 1; i >= 0; i--) {
			auto& operand = operand_list[i];
			if (!Fetch(operand, new_value)) {
				return false;
			}
			std::vector<BytesPair> exists;
			Bytes slice(operand.data(), operand.size());
			if (get_hash_bytes(slice, exists) == -1) {
				continue;
			}
			arr.reserve(arr.size() + exists.size());
			for (auto& item : exists) {
				auto it = std::find_if(arr.begin(), arr.end(), [&item](const BytesPair& p) {
					return (p.first == item.first);
				});
				if (it == arr.end()) {
					arr.emplace_back(item.first, item.second);
					len += kExtraLen;
				}
			}
		}
		std::string* str = new_value->trans_to_string();
		str->clear();
		str->reserve(len);
		for (auto& item : arr) {
			str->append(item.first.data(), item.first.size());
			str->append(item.second.data(), item.second.size());
		}
		return true;
	}

	bool IsStableMerge() const override { return true; }

	const char* Name() const override {
		return "ChessMergeOperator";
	}

};

#endif
