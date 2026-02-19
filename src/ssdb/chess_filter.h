
#ifndef CHESS_FILTER_H
#define CHESS_FILTER_H

#include "rocksdb/compaction_filter.h"
#include "t_hash.h"

class ChessCompactionFilter : public TERARKDB_NAMESPACE::CompactionFilter {
public:
	static const int16_t kMinPly = 0;

	const char* Name() const override {
		return "ChessCompactionFilter";
	}

	bool IsStableChangeValue() const override { return true; }
	bool IgnoreSnapshots() const override { return true; }

	Decision FilterV2(int /*level*/, const TERARKDB_NAMESPACE::Slice& key,
			ValueType value_type, const TERARKDB_NAMESPACE::Slice& /*existing_value_meta*/,
			const TERARKDB_NAMESPACE::LazyBuffer& existing_value,
			TERARKDB_NAMESPACE::LazyBuffer* new_value,
			std::string* /*skip_until*/) const override {
		if (key.empty() || key[0] != DataType::HASH) {
			return Decision::kKeep;
		}
		if (!existing_value.fetch().ok()) {
			return Decision::kKeep;
		}
		const TERARKDB_NAMESPACE::Slice& val = existing_value.slice();
		if (val.empty() || val.size() % (2 * sizeof(int16_t)) != 0) {
			return Decision::kKeep;
		}

		const int entry_size = 2 * sizeof(int16_t);
		const int count = val.size() / entry_size;
		bool needs_filter = false;

		for (int i = 0; i < count; i++) {
			int16_t field = *(const int16_t*)(val.data() + i * entry_size);
			int16_t value = *(const int16_t*)(val.data() + i * entry_size + sizeof(int16_t));
			if (field == kMinPly || (value_type == ValueType::kValue && value == 0x7FFF)) {
				needs_filter = true;
				break;
			}
		}

		if (!needs_filter) {
			return Decision::kKeep;
		}

		std::string* out = new_value->trans_to_string();
		out->clear();
		out->reserve(val.size());
		for (int i = 0; i < count; i++) {
			const char* entry = val.data() + i * entry_size;
			int16_t field = *(const int16_t*)entry;
			int16_t value = *(const int16_t*)(entry + sizeof(int16_t));
			if (field != kMinPly && !(value_type == ValueType::kValue && value == 0x7FFF)) {
				out->append(entry, entry_size);
			}
		}

		if (out->empty()) {
			return Decision::kRemove;
		}
		return Decision::kChangeValue;
	}
};

class ChessCompactionFilterFactory : public TERARKDB_NAMESPACE::CompactionFilterFactory {
public:
	bool prune = false;

	const char* Name() const override {
		return "ChessCompactionFilterFactory";
	}

	std::unique_ptr<TERARKDB_NAMESPACE::CompactionFilter> CreateCompactionFilter(
			const TERARKDB_NAMESPACE::CompactionFilter::Context& context) override {
		if (!prune || !context.is_manual_compaction || context.column_family_id != 0) {
			return nullptr;
		}
		return std::unique_ptr<TERARKDB_NAMESPACE::CompactionFilter>(new ChessCompactionFilter());
	}
};

#endif
