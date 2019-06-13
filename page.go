package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

// 计算page 头部大小
const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

// 每页最少两个key
const minKeysPerPage = 2

// 分支元素大小
const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))

// 叶子元素大小
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

// 页类型
const (
	// 分支
	branchPageFlag = 0x01

	// 叶子
	leafPageFlag = 0x02

	// 元信息
	metaPageFlag = 0x04

	// 空闲列表页
	freelistPageFlag = 0x10
)

// 桶叶子？
const (
	bucketLeafFlag = 0x01
)

// 页id
type pgid uint64

type page struct {
	id       pgid    // ID，uint64
	flags    uint16  // 类型
	count    uint16  // 元素数目，最大64K
	overflow uint32  // 容量
	ptr      uintptr // 数据区
}

// typ returns a human readable page type string used for debugging.
// 返回页类型
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}

	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
// 元信息页
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// leafPageElement retrieves the leaf node by index
// 叶子元素
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}

	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// branchPageElement retrieves the branch node by index
// 分支元素
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}

	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// dump writes n bytes of the page to STDERR as hex output.
// dump 十六进制
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

// 页表
type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
// 分支元素
type branchPageElement struct {
	pos   uint32 // 位置
	ksize uint32 // key 大小
	pgid  pgid   // 页ID
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))

	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// leafPageElement represents a node on a leaf page.
// 叶子
type leafPageElement struct {
	flags uint32 // 类型
	pos   uint32 // 位置
	ksize uint32 // key 大小
	vsize uint32 // 值大小
}

// key returns a byte slice of the node key.
// 返回key，第二个参数指定cap
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))

	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value returns a byte slice of the node value.
// 返回值
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))

	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	// id
	ID int

	// 类型
	Type string

	// 数目
	Count int

	// 容量
	OverflowCount int
}

// 页ID 列表
type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
// 合并页ID 列表
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func mergepgids(dst, a, b pgids) {
	// 容错
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}

	// 一些特殊情况
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	// 分主次
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// 遍历
	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		// 找比跟随者小的位置，append进merge中
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		// 交换位置
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	// 处理剩余的
	_ = append(merged, follow...)
}
