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


#pragma once
#ifndef SSHT_H_
#define SSHT_H_

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <type_traits>
#include "utils.h"

namespace ssht {

static constexpr size_t MAX_KEY_LEN = UINT8_MAX;
static constexpr size_t MAX_INLINE_VALUE_LEN = UINT16_MAX;
static constexpr unsigned MAX_VALUE_LEN_BIT = 35U;	//7x
static constexpr size_t MAX_VALUE_LEN = (1ULL<<MAX_VALUE_LEN_BIT)-1U;

enum BuildStatus {
	BUILD_STATUS_OK, BUILD_STATUS_BAD_INPUT, BUILD_STATUS_FAIL_TO_OUTPUT
};

using DataReaders = std::vector<std::unique_ptr<IDataReader>>;

//key should have fixed length
//dynamic length key is not useful, just pad or use checksum instead
extern BuildStatus BuildSet(const DataReaders& in, IDataWriter& out);

//inline large value may consume a lot of memory
extern BuildStatus BuildDict(const DataReaders& in, IDataWriter& out);

extern BuildStatus BuildDictWithVariedValue(const DataReaders& in, IDataWriter& out);


class Hashtable {
public:
	enum LoadPolicy {MAP_ONLY, MAP_FETCH, MAP_OCCUPY, COPY_DATA};
	explicit Hashtable(const std::string& path, LoadPolicy load_policy=MAP_ONLY);
	bool operator!() const noexcept { return !m_res && !m_mem; }

	enum Type : uint8_t {
		KEY_SET = 0,
		KV_INLINE = 1,
		KV_SEPARATED = 2,
		ILLEGAL_TYPE = 0xff
	};
	Type type() const noexcept { return m_view.type; }
	uint8_t key_len() const noexcept { return m_view.key_len; }
	uint16_t val_len() const noexcept { return m_view.val_len; }
	size_t item() const noexcept { return m_view.item; }


	//KEY_SET, KV_INLINE or KV_SEPARATED
	Slice search(const uint8_t* key) const noexcept;

	//KEY_SET or KV_INLINE
	//keys == out is OK
	unsigned batch_search(unsigned batch, const uint8_t* const keys[], const uint8_t* out[],
					   const Hashtable* patch=nullptr) const noexcept;

	//only KV_INLINE, if dft_val == nullptr, do nothing when miss
	unsigned batch_fetch(unsigned batch, const uint8_t* __restrict__ keys, uint8_t* __restrict__ data,
						 const uint8_t* __restrict__ dft_val=nullptr, const Hashtable* patch=nullptr) const noexcept;

	BuildStatus derive(const DataReaders& in, IDataWriter& out) const;

	struct View {
		Type type = ILLEGAL_TYPE;
		uint8_t key_len = 0;
		uint16_t val_len = 0;
		uint32_t line_size = 0; //key_len+val_len
		uint64_t seed = 0;
		uint64_t item = 0;
		Divisor<uint64_t> set_cnt;
		const uint8_t* guide = nullptr;
		const uint8_t* content = nullptr;
		const uint8_t* extend = nullptr;
		const uint8_t* space_end = nullptr;
	};

private:
	MemMap m_res;
	MemBlock m_mem;
	View m_view;
};


} //ssht
#endif //SSHT_H_