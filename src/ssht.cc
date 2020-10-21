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


#include "internal.h"

namespace ssht {

static bool CreateView(const uint8_t* addr, size_t size, Hashtable::View& out) {
	const size_t guide_off = sizeof(Header);
	if (size < guide_off) return false;

	auto header = (const Header*)addr;
	if (header->magic != SSHT_MAGIC || header->set_cnt == 0) {
		return false;
	}
	const auto slot = header->set_cnt << 6U;
	switch (header->type) {
		case Hashtable::KV_SEPARATED:
			if (header->val_len != OFFSET_FIELD_SIZE) return false;
		case Hashtable::KV_INLINE:
			if (header->val_len == 0) return false;
		case Hashtable::KEY_SET:
			if (header->key_len == 0) return false;
			break;
		default: return false;
	}
	const uint32_t line_size = header->key_len + (uint32_t)header->val_len;
	const size_t content_off = guide_off + slot;
	const size_t extend_off = content_off + slot*line_size;
	if (size < extend_off) return false;
	if (header->type == Hashtable::KV_SEPARATED) {
		if (size < extend_off + slot) return false;
	}

	out.type = (Hashtable::Type)header->type;
	out.key_len = header->key_len;
	out.val_len = header->val_len;
	out.line_size = line_size;
	out.seed = header->seed;
	out.item = header->item;
	out.set_cnt = header->set_cnt;
	out.guide = addr + guide_off;
	out.content = addr + content_off;
	out.extend = addr + extend_off;
	out.space_end = addr + size;
	return true;
}

Hashtable::Hashtable(const std::string& path, LoadPolicy load_policy) {
	if (load_policy == COPY_DATA) {
		auto mem = MemBlock::LoadFile(path.c_str());
		if (!mem || !CreateView(mem.addr(), mem.size(), m_view)) {
			return;
		}
		m_mem = std::move(mem);
	} else {
		MemMap::Policy policy = MemMap::MAP_ONLY;
		if (load_policy == MAP_FETCH) {
			policy = MemMap::FETCH;
		} else if (load_policy == MAP_OCCUPY) {
			policy = MemMap::OCCUPY;
		}
		MemMap res(path.c_str(), policy);
		if (!res || !CreateView(res.addr(), res.size(), m_view)) {
			return;
		}
		m_res = std::move(res);
	}
}

} //ssht