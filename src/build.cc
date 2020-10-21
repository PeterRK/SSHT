//==============================================================================
// A static set-associative hashtable.
// Copyright (C) 2020  Ruan Kunliang
//
// This library is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License as published by the Free
// Software Foundation; either version 2.1 of the License, or (at your option)
// any later version.
//
// This library is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
// details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the This Library; if not, see <https://www.gnu.org/licenses/>.
//==============================================================================

#include <cassert>
#include <cstring>
#include <vector>
#include <thread>
#include <exception>
#include "internal.h"

namespace ssht {

struct BuildException : public std::exception {
	const char* what() const noexcept override;
};
const char* BuildException::what() const noexcept {
	return "build exception";
}

struct InternalException : public std::exception {
	const char *what() const noexcept override;
};

const char *InternalException::what() const noexcept {
	return "this should never occur";
}

static FORCE_INLINE void Assert(bool condition) {
	if (UNLIKELY(!condition)) {
		throw InternalException();
	}
}


#define ALLOC_MEM_BLOCK(mem, size) \
	MemBlock mem(size);			\
	if (!mem) {					\
		throw std::bad_alloc();	\
	}

static uint64_t GetSeed() {
	//return 1596176575357415943ULL;
	return std::chrono::duration_cast<std::chrono::nanoseconds>(
			std::chrono::system_clock::now().time_since_epoch()).count();
}

static size_t SumInputSize(const DataReaders& in) {
	size_t total = 0;
	for (auto& reader : in) {
		total += reader->total();
	}
	return total;
}


template <typename Fill>
static FORCE_INLINE bool Mapping(uint8_t* guide, uint8_t* space, const Header& header,
								 const Divisor<uint64_t>& set_cnt, const uint8_t* key, const Fill& fill) {
	const unsigned line_size = header.key_len + (unsigned)header.val_len;
	auto [set, mark, sft] = HashKey(key, header.key_len, header.seed, set_cnt);
	while (true) {
		auto g = guide + (set<<6U);
		for (unsigned j = sft; j < sft+64U; j++) {
			auto off = j & 63U;
			auto line = space + ((set<<6U)+off)*line_size;
			auto m = LoadRelaxed(g[off]);
			if (m == 0xffU && UpdateCAS(g[off], m, (uint8_t)0x80U)) {
				fill(line);
				StoreRelease(g[off], mark);
				return true;
			}
			while (m & 0x80U) {
				NanoSleep();
				m = LoadRelaxed(g[off]);
			}
			if (UNLIKELY(m == mark && Equal(line, key, header.key_len))) {
				return false;
			}
		}
		if (++set >= set_cnt.value()) {
			set = 0;
		}
	}
}

static NOINLINE size_t Mapping(uint8_t* guide, uint8_t* space, const Header& header, IDataReader& reader) {
	Divisor<uint64_t> set_cnt(header.set_cnt);
	auto total = reader.total();
	size_t cnt = total;
	for (size_t i = 0; i < total; i++) {
		auto rec = reader.read(false);
		if (rec.key.ptr == nullptr || rec.key.len != header.key_len
			|| (header.val_len != 0 && (rec.val.ptr == nullptr || rec.val.len != header.val_len))) {
			throw BuildException();
		}
		if (!Mapping(guide, space, header, set_cnt, rec.key.ptr, [&rec, &header](uint8_t* line){
			Assign(line, rec.key.ptr, header.key_len);
			if (header.val_len != 0) {
				memcpy(line+header.key_len, rec.val.ptr, header.val_len);
			}
		})) cnt--;
	}
	return cnt;
}

struct BasicInfo {
	Hashtable::Type type;
	uint8_t key_len;
	uint16_t val_len;
};

static size_t CalcSetCnt(size_t item) {
	const auto reserved = (item+(RESERVE_FACTOR-1))/RESERVE_FACTOR;
	return (((item+reserved+63U)/64U)&(~1ULL))+1U;
}

static BuildStatus BuildWithFixedSizeValue(const BasicInfo& info, const DataReaders& in, IDataWriter& out) {
	auto total = SumInputSize(in);
	Assert(!in.empty() && info.key_len != 0 && total != 0);

	Header header;
	header.type = info.type;
	header.key_len = info.key_len;
	header.val_len = info.val_len;
	header.seed = GetSeed();
	header.set_cnt = CalcSetCnt(total);
	const auto slot = header.set_cnt << 6U;

	ALLOC_MEM_BLOCK(guide, slot)
	memset(guide.addr(), 0xff, slot);

	const auto line_size = header.key_len + (uint32_t)header.val_len;
	ALLOC_MEM_BLOCK(space, slot*line_size);

	bool fail = false;
	std::vector<std::thread> threads;
	threads.reserve(in.size());

	for (auto& reader : in) {
		reader->reset();
		threads.emplace_back([&fail, &guide, &space, &header](IDataReader* reader) {
			try {
				auto cnt = Mapping(guide.addr(), space.addr(), header, *reader);
				AddRelaxed(header.item, cnt);
			} catch (const BuildException&) {
				fail = true;
			}
		}, reader.get());
	}
	for (auto& t : threads) {
		t.join();
	}
	if (fail) {
		return BUILD_STATUS_BAD_INPUT;
	}

	if (!out.write(&header, sizeof(header))
		|| !out.write(guide.addr(), guide.size())
		|| !out.write(space.addr(), space.size())
	) return BUILD_STATUS_FAIL_TO_OUTPUT;
	return BUILD_STATUS_OK;
}

static bool DetectKeyValueLen(IDataReader& reader, uint8_t* key_len, uint16_t* val_len) {
	if (key_len == nullptr) {
		return false;
	}
	auto rec = reader.read(val_len == nullptr);
	if (rec.key.ptr == nullptr || rec.key.len == 0 || rec.key.len > MAX_KEY_LEN) {
		return false;
	}
	*key_len =  rec.key.len;
	if (val_len != nullptr) {
		if (rec.val.ptr == nullptr || rec.val.len == 0 || rec.val.len > MAX_INLINE_VALUE_LEN) {
			return false;
		}
		*val_len = rec.val.len;
	}
	reader.reset();
	return true;
}

BuildStatus BuildSet(const DataReaders& in, IDataWriter& out) {
	uint8_t key_len;
	if (in.empty() || !(DetectKeyValueLen(*in.front(), &key_len, nullptr))) {
		return BUILD_STATUS_BAD_INPUT;
	}
	return BuildWithFixedSizeValue( {Hashtable::KEY_SET, key_len, 0}, in, out);
}

extern BuildStatus BuildDict(const DataReaders& in, IDataWriter& out) {
	uint8_t key_len;
	uint16_t val_len;
	if (in.empty() || !(DetectKeyValueLen(*in.front(), &key_len, &val_len))) {
		return BUILD_STATUS_BAD_INPUT;
	}
	return BuildWithFixedSizeValue({Hashtable::KV_INLINE, key_len, val_len}, in, out);
}

static unsigned VarIntSize(size_t n) {
	unsigned cnt = 1;
	while ((n & ~0x7fULL) != 0) {
		n >>= 7U;
		cnt++;
	}
	return cnt;
}
static bool WriteVarInt(size_t n, IDataWriter& out) {
	uint8_t buf[10];
	unsigned w = 0;
	while ((n & ~0x7fULL) != 0) {
		buf[w++] = 0x80ULL | (n & 0x7fULL);
		n >>= 7U;
	}
	buf[w++] = n;
	return out.write(buf, w);
}

class KeyOffReader : public IDataReader {
public:
	explicit KeyOffReader(IDataReader& core, size_t off)
		: m_core(core), m_base(off), m_offset(m_base) {}

	void reset() override {
		m_core.reset();
		m_offset = m_base;
	}
	size_t total() override {
		return m_core.total();
	}
	Record read(bool) override {
		auto rec = m_core.read(false);
		if (m_offset > MAX_OFFSET || rec.val.len > MAX_VALUE_LEN || (rec.val.len != 0 && rec.val.ptr == nullptr)) {
			throw BuildException();
		}
		WriteOffsetField(m_field, m_offset);
		m_offset += VarIntSize(rec.val.len) + rec.val.len;
		rec.val.ptr = m_field;
		rec.val.len = OFFSET_FIELD_SIZE;
		return rec;
	}
	size_t offset() const noexcept { return m_offset; }

private:
	IDataReader& m_core;
	const size_t m_base;
	size_t m_offset;
	uint8_t m_field[OFFSET_FIELD_SIZE];
};

static FORCE_INLINE bool DumpVariedValue(const Slice& val, IDataWriter& out) {
	return WriteVarInt(val.len, out) && (val.len == 0 || out.write(val.ptr, val.len));
}

BuildStatus BuildDictWithVariedValue(const DataReaders& in, IDataWriter& out) {
	Header header;
	if (in.empty() || !(DetectKeyValueLen(*in.front(), &header.key_len, nullptr))) {
		return BUILD_STATUS_BAD_INPUT;
	}
	header.type = Hashtable::KV_SEPARATED;
	header.val_len = OFFSET_FIELD_SIZE;
	header.seed = GetSeed();

	const auto total = SumInputSize(in);
	header.set_cnt = CalcSetCnt(total);
	const auto slot = header.set_cnt << 6U;

	ALLOC_MEM_BLOCK(guide, slot)
	memset(guide.addr(), 0xff, slot);

	const auto line_size = header.key_len + OFFSET_FIELD_SIZE;
	ALLOC_MEM_BLOCK(space, slot*line_size);

	size_t offset = 0;
	for (auto& reader : in) {
		reader->reset();
		KeyOffReader wrapped_reader(*reader, offset);
		try {
			header.item += Mapping(guide.addr(), space.addr(), header, wrapped_reader);
		} catch (const BuildException&) {
			return BUILD_STATUS_BAD_INPUT;
		}
		offset = wrapped_reader.offset();
	}
	if (header.item != total) {
		return BUILD_STATUS_BAD_INPUT;
	}
	if (!out.write(&header, sizeof(header))
		|| !out.write(guide.addr(), guide.size())
		|| !out.write(space.addr(), space.size())
	) return BUILD_STATUS_FAIL_TO_OUTPUT;

	guide = MemBlock{};
	space = MemBlock{};

	for (auto& reader : in) {
		reader->reset();
		auto cnt = reader->total();
		for (size_t i = 0; i < cnt; i++) {
			if (!DumpVariedValue(reader->read(false).val, out)) {
				return BUILD_STATUS_FAIL_TO_OUTPUT;
			}
		}
	}
	return BUILD_STATUS_OK;
}


static size_t CountHit(const Hashtable::View& base, IDataReader& reader) {
	size_t hit = 0;
	auto total = reader.total();
	for (size_t i = 0; i < total; i++) {
		auto key = reader.read(true).key;
		if (key.ptr == nullptr || key.len != base.key_len) {
			throw BuildException();
		}
		if (Search(base, key.ptr) != nullptr) {
			hit++;
		}
	}
	reader.reset();
	return hit;
}

static BuildStatus RebuildWithFixedSizeValue(const Hashtable::View& base, const DataReaders& in, IDataWriter& out) {
	Assert(!in.empty() && (base.type == Hashtable::KEY_SET || base.type == Hashtable::KV_INLINE));
	std::vector<std::thread> threads;
	threads.reserve(in.size());

	bool fail = false;
	size_t dirty = 0;
	for (auto& reader : in) {
		reader->reset();
		threads.emplace_back([&base, &fail, &dirty](IDataReader* reader){
			try {
				auto hit = CountHit(base, *reader);
				AddRelaxed(dirty, hit);
			} catch (const BuildException&) {
				fail = true;
				return;
			}
		}, reader.get());
	}
	for (auto& t : threads) {
		t.join();
	}
	if (fail) {
		return BUILD_STATUS_BAD_INPUT;
	}

	Assert(dirty <= base.item);
	const auto total = SumInputSize(in) + base.item - dirty;

	Header header;
	header.type = base.type;
	header.key_len = base.key_len;
	header.val_len = base.val_len;
	header.seed = GetSeed();
	header.set_cnt = CalcSetCnt(total);
	const auto slot = header.set_cnt << 6U;

	ALLOC_MEM_BLOCK(guide, slot)
	memset(guide.addr(), 0xff, slot);
	ALLOC_MEM_BLOCK(space, slot*base.line_size);

	threads.clear();
	fail = false;
	for (auto& reader : in) {
		threads.emplace_back([&fail, &guide, &space, &header](IDataReader* reader){
			size_t cnt = 0;
			try {
				cnt = Mapping(guide.addr(), space.addr(), header, *reader);
			} catch (const BuildException&) {
				fail = true;
				return;
			}
			AddRelaxed(header.item, cnt);
		}, reader.get());
	}
	for (auto& t : threads) {
		t.join();
	}
	if (fail) {
		return BUILD_STATUS_BAD_INPUT;
	}

	threads.clear();
	const auto piece = (base.set_cnt.value()<<6U) / in.size();
	const auto remain = (base.set_cnt.value()<<6U) % in.size();
	size_t off = 0;
	for (unsigned i = 0; i < in.size(); i++) {
		size_t begin = off;
		off += i<remain ? piece+1 : piece;
		threads.emplace_back([&base, &guide, &space, &header](size_t begin, size_t end){
			size_t cnt = 0;
			Divisor<uint64_t> set_cnt(header.set_cnt);
			auto line = base.content + begin*base.line_size;
			for (size_t i = begin; i < end; i++) {
				if ((base.guide[i] & 0x80U) == 0
					&& Mapping(guide.addr(), space.addr(), header, set_cnt, line, [line, &base](uint8_t* out){
					memcpy(out, line, base.line_size);
				})) cnt++;
				line += base.line_size;
			}
			AddRelaxed(header.item, cnt);
		}, begin, off);
	}
	for (auto& t : threads) {
		t.join();
	}

	if (!out.write(&header, sizeof(header))
		|| !out.write(guide.addr(), guide.size())
		|| !out.write(space.addr(), space.size())
	) return BUILD_STATUS_FAIL_TO_OUTPUT;
	return BUILD_STATUS_OK;
}

static BuildStatus RebuildDictWithVariedValue(const Hashtable::View& base, const DataReaders& in, IDataWriter& out) {
	Assert(!in.empty() && (base.type == Hashtable::KV_SEPARATED));

	size_t dirty = 0;
	for (auto& reader : in) {
		reader->reset();
		try {
			dirty += CountHit(base, *reader);
		} catch (const BuildException&) {
			return BUILD_STATUS_BAD_INPUT;
		}
	}

	Assert(dirty <= base.item);
	auto neo = SumInputSize(in);
	const auto total = base.item + neo - dirty;

	Header header;
	header.type = base.type;
	header.key_len = base.key_len;
	header.val_len = base.val_len;
	header.seed = GetSeed();
	header.set_cnt = CalcSetCnt(total);
	const auto slot = header.set_cnt << 6U;

	ALLOC_MEM_BLOCK(guide, slot)
	memset(guide.addr(), 0xff, slot);
	ALLOC_MEM_BLOCK(space, slot*base.line_size);

	size_t offset = 0;
	for (auto& reader : in) {
		KeyOffReader wrapped_reader(*reader, offset);
		try {
			header.item += Mapping(guide.addr(), space.addr(), header, wrapped_reader);
		} catch (const BuildException&) {
			return BUILD_STATUS_BAD_INPUT;
		}
		offset = wrapped_reader.offset();
	}
	if (header.item != neo) {
		return BUILD_STATUS_BAD_INPUT;
	}

	const Divisor<uint64_t> set_cnt(header.set_cnt);
	const auto base_slot = base.set_cnt.value() << 6U;
	ALLOC_MEM_BLOCK(mem, (base_slot+7U)/8U)
	memset(mem.addr(), 0, mem.size());
	auto bitmap = mem.addr();

	auto line = base.content;
	for (size_t i = 0; i < base_slot; i++) {
		if ((base.guide[i] & 0x80U) == 0
			&& Mapping(guide.addr(), space.addr(), header, set_cnt, line,
					   [line, &base, &offset](uint8_t* out) {
						   Assign(out, line, base.key_len);
						   auto val = SeparatedValue(base.extend+ReadOffsetField(line+base.key_len), base.space_end);
						   Assert(val.ptr != nullptr);
						   WriteOffsetField(out+base.key_len, offset);
						   offset += VarIntSize(val.len) + val.len;
					   })
		) {
			header.item++;
			SetBit(bitmap, i);
		}
		line += base.line_size;
	}

	if (!out.write(&header, sizeof(header))
		|| !out.write(guide.addr(), guide.size())
		|| !out.write(space.addr(), space.size())
	) return BUILD_STATUS_FAIL_TO_OUTPUT;

	guide = MemBlock{};
	space = MemBlock{};

	for (auto& reader : in) {
		reader->reset();
		auto cnt = reader->total();
		for (size_t i = 0; i < cnt; i++) {
			if (!DumpVariedValue(reader->read(false).val, out)) {
				return BUILD_STATUS_FAIL_TO_OUTPUT;
			}
		}
	}
	auto field = base.content + base.key_len;
	for (size_t i = 0; i < base_slot; i++) {
		if (TestBit(bitmap, i)
			&& !DumpVariedValue(SeparatedValue(base.extend+ReadOffsetField(field), base.space_end), out)) {
			return BUILD_STATUS_FAIL_TO_OUTPUT;
		}
		field += base.line_size;
	}
	return BUILD_STATUS_OK;
}

BuildStatus Hashtable::derive(const DataReaders& in, IDataWriter& out) const {
	if (!*this || in.empty()) {
		return BUILD_STATUS_BAD_INPUT;
	}
	switch (m_view.type) {
		case Hashtable::KEY_SET:
		case Hashtable::KV_INLINE:
			return RebuildWithFixedSizeValue(m_view, in, out);
		case Hashtable::KV_SEPARATED:
			return RebuildDictWithVariedValue(m_view, in, out);
		default:
			return BUILD_STATUS_BAD_INPUT;
	}
}

} //ssht
