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
//#ifndef SSHT_INTERNAL_H_
//#define SSHT_INTERNAL_H_

#include <cstring>
#include <tuple>
#include <functional>
#include <type_traits>
#include <ssht.h>

#define FORCE_INLINE inline __attribute__((always_inline))
#define NOINLINE __attribute__((noinline))

#define LIKELY(exp) __builtin_expect((exp),1)
#define UNLIKELY(exp) __builtin_expect((exp),0)

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "little endian only"
#endif

namespace ssht {

extern uint64_t Hash(const uint8_t* msg, uint8_t len, uint64_t seed) noexcept;

static FORCE_INLINE std::tuple<uint64_t,uint8_t,uint8_t>
HashKey(const uint8_t* key, uint8_t len, uint64_t seed, const Divisor<uint64_t>& set_cnt) {
	const uint64_t hash = Hash(key, len, seed);
	const uint64_t set = hash % set_cnt;
	const uint8_t mark = (hash >> 51U) & 0x7fU;
	const uint8_t sft = hash >> 58U;
	return {set, mark, sft};
}

static FORCE_INLINE void PrefetchForNext(const void* ptr) {
	__builtin_prefetch(ptr, 0, 3);
}
static FORCE_INLINE void PrefetchForFuture(const void* ptr) {
	__builtin_prefetch(ptr, 0, 0);
}

static FORCE_INLINE void NanoSleep() {
#if defined(__amd64__) || defined(__i386__)
	__asm__ volatile ("pause");
#elif defined(__aarch64__) || defined(__arm__)
	__asm__ volatile ("yield");
#endif
}

static FORCE_INLINE bool TestBit(const uint8_t bitmap[], size_t pos) {
	return (bitmap[pos>>3U] & (1U<<(pos&7U))) != 0;
}

static FORCE_INLINE void SetBit(uint8_t bitmap[], size_t pos) {
	bitmap[pos>>3U] |= (1U<<(pos&7U));
}

static FORCE_INLINE void ClearBit(uint8_t bitmap[], size_t pos) {
	bitmap[pos>>3U] &= ~(1U<<(pos&7U));
}

static FORCE_INLINE bool TestAndSetBit(uint8_t bitmap[], size_t pos) {
	auto& b = bitmap[pos>>3U];
	const uint8_t m = 1U << (pos&7U);
	if (b & m) {
		return false;
	}
	b |= m;
	return true;
}

//optimize for common short cases
static FORCE_INLINE bool Equal(const uint8_t* a, const uint8_t* b, uint8_t len) {
	if (len == sizeof(uint64_t)) {
		return *(const uint64_t*)a == *(const uint64_t*)b;
	} else if (len == sizeof(uint32_t)) {
		return *(const uint32_t*)a == *(const uint32_t*)b;
	} else {
		return memcmp(a, b, len) == 0;
	}
}
static FORCE_INLINE void Assign(uint8_t* dest, const uint8_t* src, uint8_t len) {
	if (len == sizeof(uint64_t)) {
		*(uint64_t*)dest = *(const uint64_t*)src;
	} else if (len == sizeof(uint32_t)) {
		*(uint32_t*)dest = *(const uint32_t*)src;
	} else {
		memcpy(dest, src, len);
	}
}

template <typename T>
T FORCE_INLINE LoadRelaxed(const T& tgt) {
	return __atomic_load_n(&tgt, __ATOMIC_RELAXED);
}

template <typename T>
void FORCE_INLINE StoreRelease(T& tgt, T val) {
	__atomic_store_n(&tgt, val, __ATOMIC_RELEASE);
}

template <typename T>
T FORCE_INLINE AddRelaxed(T& tgt, T val) {
	return __atomic_fetch_add(&tgt, val, __ATOMIC_RELAXED);
}

template <typename T>
bool FORCE_INLINE UpdateCAS(T& tgt, T old, T val) {
	return __atomic_compare_exchange_n(&tgt, &old, val, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED);
}


static constexpr uint32_t OFFSET_FIELD_SIZE = 6;
static constexpr uint64_t MAX_OFFSET = (1ULL<<(OFFSET_FIELD_SIZE*8U))-1;

static FORCE_INLINE size_t ReadOffsetField(const uint8_t* field) {
	return (((uint64_t)*(uint16_t*)(field+4))<<32U) | *(uint32_t*)field;
}

static FORCE_INLINE void WriteOffsetField(uint8_t* field, size_t offset) {
	*(uint32_t*)field = offset;
	*(uint16_t*)(field+4) = offset>>32U;
}

static constexpr unsigned RESERVE_FACTOR = 16;

static constexpr uint32_t SSHT_MAGIC = 0x54485353;

struct Header {
	uint32_t magic = SSHT_MAGIC;
	uint8_t type = Hashtable::ILLEGAL_TYPE;
	uint8_t key_len = 0;
	uint16_t val_len = 0;
	uint64_t seed = 0;
	uint64_t item = 0;
	uint64_t set_cnt = 0;
	uint8_t _pad[32];
};

static_assert(sizeof(Header)==64);

extern Slice SeparatedValue(const uint8_t* pt, const uint8_t* end) noexcept;

extern const uint8_t* Search(const Hashtable::View& pack, const uint8_t* key) noexcept;

} //ssht
//#endif //SSHT_INTERNAL_H_
