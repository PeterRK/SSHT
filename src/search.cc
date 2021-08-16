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
#include <algorithm>
#include "internal.h"

namespace ssht {

Slice SeparatedValue(const uint8_t* pt, const uint8_t* end) noexcept {
	static_assert(MAX_VALUE_LEN_BIT % 7U == 0, "MAX_VALUE_LEN_BIT should be 7x");

	uint64_t len = 0;
	for (unsigned sft = 0; sft < MAX_VALUE_LEN_BIT; sft += 7U) {
		if (pt >= end) {
			return {nullptr, 0};
		}
		uint8_t b = *pt++;
		if (b & 0x80U) {
			len |= static_cast<uint64_t>(b & 0x7fU) << sft;
		} else {
			len |= static_cast<uint64_t>(b) << sft;
			if (pt+len > end) {
				return {};
			}
			return {pt, len};
		}
	}
	return {};
}

#define ENABLE_STEP8

#ifdef ENABLE_STEP8
static FORCE_INLINE uint64_t CalcHint(uint64_t vec, uint8_t mark) {
	const uint64_t vone = 0x101010101010101ULL;
	const uint64_t vsign = 0x8080808080808080ULL;
	const uint64_t vmark = ~(vone*mark);
	const uint64_t match = (vec^vsign) & vsign & (((vec^vmark)&~vsign)+vone);
	const uint64_t empty = vec & vsign;
	return empty | match;
}
#endif

const uint8_t* Search(const Hashtable::View& pack, const uint8_t* key) noexcept {
	assert(key != nullptr);
	auto[set, mark, sft] = HashKey(key, pack.key_len, pack.seed, pack.set_cnt);

#define HANDLE_SLOT(pos) \
	if (g[pos] == mark) {													\
		auto line = pack.content + ((set << 6U) + (pos)) * pack.line_size;	\
		if (Equal(key, line, pack.key_len)) {								\
			return line + pack.key_len;										\
		}																	\
	} else if (g[pos] & 0x80U) {											\
		return nullptr;														\
	}

	while (true) {
		auto g = pack.guide + (set << 6U);
		for (unsigned j = sft; j < sft+64U;) {
			auto off = j & 63U;
#ifdef ENABLE_STEP8
			if (j <= sft+56U && off <= 56U) {
				for (auto hint = CalcHint(*(uint64_t*)(g+off), mark); hint != 0; hint &= (hint-1)) {
					auto skip = ((__builtin_ctzll(hint)+1U)>>3U)-1U;
					HANDLE_SLOT(off + skip)
				}
				j += 8U;
				continue;
			}
#endif
			HANDLE_SLOT(off)
			j++;
		}
		if (++set >= pack.set_cnt.value()) {
			set = 0;
		}
	}

#undef HANDLE_SLOT
}

Slice Hashtable::search(const uint8_t* key) const noexcept {
	if (!*this || key == nullptr) {
		return {};
	}
	auto field = Search(m_view, key);
	if (field == nullptr) {
		return {};
	}
	if (m_view.type != KV_SEPARATED) {
		return {field, m_view.val_len};
	}
	auto off = ReadOffsetField(field);
	return SeparatedValue(m_view.extend+off, m_view.space_end);
}


#ifndef CACHE_BLOCK_SIZE
#define CACHE_BLOCK_SIZE 64U
#endif
static_assert(CACHE_BLOCK_SIZE >= 64U && (CACHE_BLOCK_SIZE&(CACHE_BLOCK_SIZE-1)) == 0);


template <typename GetKey, typename FillVal>
static FORCE_INLINE unsigned BatchProcess(unsigned batch, const Hashtable::View& base, const Hashtable::View* patch,
										  const GetKey& get_key, const FillVal& fill_val, const uint8_t* dft_val=nullptr) noexcept {
	if ((base.type == Hashtable::KV_SEPARATED) || (patch != nullptr &&
		(patch->type != base.type || patch->key_len != base.key_len || patch->val_len != base.val_len))) {
		return 0;
	}
	if (patch == &base) {
		patch = nullptr;
	}

	constexpr unsigned WINDOW_SIZE = 16;
	struct State {
		unsigned idx;
		uint8_t sft;
		uint8_t cur;
		uint8_t mark;
		uint64_t set;
		const uint8_t* line;
		const Hashtable::View* pack;
	} states[WINDOW_SIZE];

	unsigned hit = 0;
	auto window = std::min(batch, WINDOW_SIZE);

	auto bind_pipeline = [&get_key](const Hashtable::View* pack, State& state) {
		state.pack = pack;
		auto [set, mark, sft] = HashKey(get_key(state.idx), state.pack->key_len, state.pack->seed, state.pack->set_cnt);
		state.set = set;
		state.mark = mark;
		state.sft = sft;
		state.cur = sft;
		state.line = nullptr;
		PrefetchForNext(state.pack->guide + (state.set << 6U));
	};
	auto init_pipeline = [&base, patch, &bind_pipeline](State& state, unsigned idx) {
		state.idx = idx;
		bind_pipeline(patch==nullptr? &base : patch, state);
	};

	const auto key_len = base.key_len;
	const auto line_size = base.line_size;
	auto prefetch_line = [key_len, line_size](const uint8_t* line) {
		PrefetchForNext(line);
		auto off = (uintptr_t)line & (CACHE_BLOCK_SIZE-1);
		auto blk = (const void*)(((uintptr_t)line & ~(uintptr_t)(CACHE_BLOCK_SIZE-1)) + CACHE_BLOCK_SIZE);
		if (off + key_len > CACHE_BLOCK_SIZE) {
			PrefetchForNext(blk);
		} else if (off + line_size > CACHE_BLOCK_SIZE) {
			PrefetchForFuture(blk);
		}
	};

	unsigned idx = 0;
	for (; idx < window; idx++) {
		init_pipeline(states[idx], idx);
	}
	while (window > 0) {
		for (unsigned i = 0; i < window;) {
			auto& st = states[i];
			if (st.line != nullptr) {
				if (Equal(get_key(st.idx), st.line, key_len)) {
					hit++;
					fill_val(st.idx, st.line+key_len);
					goto reload;
				}
				st.line = nullptr;
			} else {
				auto g = st.pack->guide + (st.set<<6U);
				while (st.cur < st.sft+64U) {
					auto off = st.cur & 63U;
#ifdef ENABLE_STEP8
					if (st.cur <= st.sft+56U && off <= 56U) {
						auto hint = CalcHint(*(uint64_t*)(g+off), st.mark);
						if (hint == 0) {
							st.cur += 8U;
							continue;
						} else {
							auto step = ((__builtin_ctzll(hint)+1U)>>3U);
							off += step-1U;
							st.cur += step;
						}
					} else
#endif
						st.cur++;
					if (g[off] == st.mark) {
						st.line = st.pack->content + ((st.set<<6U)+off)*line_size;
						prefetch_line(st.line);
						goto next;
					} else if (g[off] & 0x80U) {
						if (st.pack == patch) {
							bind_pipeline(&base, st);
							goto next;
						} else {
							fill_val(st.idx, dft_val);
						}
						goto reload;
					}
				}
				//miss in set
				st.cur = st.sft;
				if (++st.set >= st.pack->set_cnt.value()) {
					st.set = 0;
				}
				PrefetchForNext(st.pack->guide + (st.set<<6U));
			}
		next:
			i++;
			continue;

		reload:
			if (idx < batch) {
				init_pipeline(st, idx++);
				i++;
			} else {
				st = states[--window];
			}
		}
	}
	return hit;
}


unsigned Hashtable::batch_search(unsigned batch, const uint8_t* const keys[], const uint8_t* out[],
								 const Hashtable* patch) const noexcept {
	if (!*this || keys == nullptr || out == nullptr) {
		return 0;
	}
	return BatchProcess(batch, m_view, patch==nullptr? nullptr : &patch->m_view,
						[keys](unsigned idx)->const uint8_t*{
							return keys[idx];
						},
						[out](unsigned idx, const uint8_t* val) {
							out[idx] = val;
						});
}

unsigned Hashtable::batch_fetch(unsigned batch, const uint8_t* __restrict__ keys, uint8_t* __restrict__ data,
								const uint8_t* __restrict__ dft_val, const Hashtable* patch) const noexcept {
	if (!*this || keys == nullptr || data == nullptr || m_view.type != Hashtable::KV_INLINE) {
		return 0;
	}
	const unsigned key_len = m_view.key_len;
	const unsigned val_len = m_view.val_len;
	return BatchProcess(batch, m_view, patch==nullptr? nullptr : &patch->m_view,
						[keys, key_len](unsigned idx)->const uint8_t*{
							return keys + idx*key_len;
						},
						[data, val_len](unsigned idx, const uint8_t* val) {
							auto out = data + idx*val_len;
							if (val != nullptr) {
								memcpy(out, val, val_len);
							}
						}, dft_val);
}

} //ssht
