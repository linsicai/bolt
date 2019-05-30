package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
// 事务会使用页，使用完后放在这里，下次直接从这里申请
type freelist struct {
	// 空闲页
	ids []pgid // all free and available free page ids.
	// 待处理页
	pending map[txid][]pgid // mapping of soon-to-be free page ids by tx.
	// 页表
	cache map[pgid]bool // fast lookup of all free and pending page ids.
}

// newFreelist returns an empty, initialized freelist.
// 创建函数
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// size returns the size of the page after serialization.
// 计算空闲页的物理大小
// 页头大小 + id 数 * id 大小
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		// 多用一个存储数目
		// The first element will be used to store the count. See freelist.write.
		n++
	}

	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist
// 计算总id 数
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// free_count returns count of free pages
// 空闲数
func (f *freelist) free_count() int {
	return len(f.ids)
}

// pending_count returns count of pending pages
// 待处理数
func (f *freelist) pending_count() int {
	var count int

	for _, list := range f.pending {
		count += len(list)
	}

	return count
}

// copyall copies into dst a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
// 复制所有id
func (f *freelist) copyall(dst []pgid) {
	// 取pending，做排序
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)

	// 合并
	mergepgids(dst, f.ids, m)
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
// 申请n个连续页
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		// 无空间可用
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			// 异常id，0、1号页是元信息页
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		// 找连续页
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		if (id-initial)+1 == pgid(n) {
			// 找到了

			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n {
				// 没有空洞
				f.ids = f.ids[i+1:]
			} else {
				// 从中间取的，右边复制到左边，然后resize
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// Remove from the free cache.
			// 不再是空闲页
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			// 返回开始页
			return initial
		}

		previd = id
	}

	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
// 释放页到事务中
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		// 异常页
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// Free page and all its overflow pages.
	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// Verify that page is not already free.
		if f.cache[id] {
			// 已经释放过了？
			panic(fmt.Sprintf("page %d already freed", id))
		}

		// Add to the freelist and cache.
		// 添加到空闲页中
		ids = append(ids, id)
		f.cache[id] = true
	}

	f.pending[txid] = ids
}

// release moves all page ids for a transaction id (or older) to the freelist.
// 事务资源回收，从待处理到空闲
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)

	// 遍历待处理表，将老的事务回收
	for tid, ids := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}

	// 回收
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// rollback removes the pages from a given pending tx.
// 事务回滚，清理缓存
func (f *freelist) rollback(txid txid) {
	// Remove page ids from cache.
	// 从缓存中删除
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	// Remove pages from pending list.
	// 清理事务缓存表
	delete(f.pending, txid)
}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		// 溢出了，从内容读取个数
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		// 空页
		f.ids = nil
	} else {
		// 从页中复制ID 列表
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// Make sure they're sorted.
		// 排序
		sort.Sort(pgids(f.ids))
	}

	// Rebuild the page cache.
	// 重建索引
	f.reindex()
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
// 写到页中
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	// 设置标记
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	// 写数目和内容
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		// 数目 + id 列表
		p.count = uint16(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		// 标记 + 数目 + id 列表
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
// 重新加载
func (f *freelist) reload(p *page) {
	// 读取页信息
	f.read(p)

	// Build a cache of only pending pages.
	// 获取待处理map
	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	// 剔除在待处理的id
	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	// Once the available list is rebuilt then rebuild the free cache so that
	// it includes the available and pending free pages.
	// 重建索引
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
// 重建索引
func (f *freelist) reindex() {
	// 缓存所有空闲id
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}

	// 缓存所有事务id
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
