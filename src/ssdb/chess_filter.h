
#ifndef CHESS_FILTER_H
#define CHESS_FILTER_H

#include "rocksdb/compaction_filter.h"
#include "ssdb.h"
#include "ssdb_impl.h"
#include "t_hash.h"

class ChessCompactionFilter : public rocksdb::CompactionFilter {
public:
	ChessCompactionFilter() {}
	
	virtual ~ChessCompactionFilter() { }

	virtual Decision FilterV2(int level, const rocksdb::Slice& key, ValueType value_type,
		const rocksdb::Slice& existing_value, std::string* new_value,
		std::string* /*skip_until*/) const {
		switch (value_type) {
			case ValueType::kValue: {
				if (existing_value.size() == 0 || existing_value.size() % (2 * sizeof(int16_t)) != 0) {
					return Decision::kRemove;
				}
				bool shoud_keep = false;
				for (int i = 0; i < existing_value.size(); i += 2 * sizeof(int16_t)) {
					if(*(int16_t*)(existing_value.data() + i) == 0
						|| *(int16_t*)(existing_value.data() + i + sizeof(int16_t)) == 0x7FFF)
						continue;
					shoud_keep = true;
					break;
				}
				return shoud_keep ? Decision::kKeep : Decision::kRemove;
			}
			case ValueType::kMergeOperand: {
				return Decision::kKeep;
			}
		}
		assert(false);
		return Decision::kKeep;
	}

	virtual const char* Name() const {
		return "ChessCompactionFilter";
	}

};

#endif
