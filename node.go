package bolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	// 桶
	bucket *Bucket

	// 是否叶子
	isLeaf bool

	// 是否不平衡
	unbalanced bool

	// ？
	spilled bool

	// 第一个key
	key []byte

	// 页ID
	pgid pgid

	// 父节点
	parent *node
	// 子节点列表
	children nodes

	// 内部节点列表
	inodes inodes
}

// root returns the top-level node this node is attached to.
// 递归找根节点
func (n *node) root() *node {
	if n.parent == nil {
		// 本节点
		return n
	}

	// 父节点的根节点
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
// 返回最少key 数目
func (n *node) minKeys() int {
	if n.isLeaf {
		// 叶子节点最小一个
		return 1
	}

	// 其他最少两个
	return 2
}

// size returns the size of the node after serialization.
// 总大小 = 页头大小 + Sum(i节点大小)
//
// i节点大小 = 元素大小 + key 大小 + value 大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()

	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}

	return sz
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
// 校验指定大小是否合适
func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()

	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]

		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			// 空间不够
			return false
		}
	}

	// 空间足够
	return true
}

// pageElementSize returns the size of each page element based on the type of node.
// 元素大小
func (n *node) pageElementSize() int {
	if n.isLeaf {
		// 叶子节点
		return leafPageElementSize
	}

	// 分支节点
	return branchPageElementSize
}

// childAt returns the child node at a given index.
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		// 叶子节点，抛异常
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}

	// index -> pgid
	// pgid -> 页
	// 页 -> node
	// 将node 加入子节点列表中
	return n.bucket.node(n.inodes[index].pgid, n)
}

// childIndex returns the index of a given child node.
// 找到第一个>= child 节点值的序号
func (n *node) childIndex(child *node) int {
	index := sort.Search(
		len(n.inodes),
		func(i int) bool {
			return bytes.Compare(n.inodes[i].key, child.key) != -1
		}
	)

	return index
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
// 找邻居节点
func (n *node) nextSibling() *node {
	if n.parent == nil {
		// 父节点为空
		return nil
	}

	// 定位节点序号
	index := n.parent.childIndex(n)

	if index >= n.parent.numChildren() - 1 {
		// 已经是最右边
		return nil
	}

	// 序号++
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
func (n *node) prevSibling() *node {
	if n.parent == nil {
		// 父节点为空
		return nil
	}

	// 定位节点序号
	index := n.parent.childIndex(n)
	if index == 0 {
		// 已经是最左边
		return nil
	}

	// 序号--
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	// 异常检测
	if pgid >= n.bucket.tx.meta.pgid {
		// 页id 异常
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		// key 为空
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		// value 为空
		panic("put: zero-length new key")
	}

	// Find insertion index.
	// 定位oldKey 位置
	index := sort.Search(
		len(n.inodes),
		func(i int) bool {
			return bytes.Compare(n.inodes[i].key, oldKey) != -1
		}
	)

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	// 是否精确命中
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		// 非精确命中

		// 扩节点
		n.inodes = append(n.inodes, inode{})

		// 移动节点
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	// 节点初始化
	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid

	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	// 定位节点
	index := sort.Search(
		len(n.inodes),
		func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		}
	)

	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		// 没有找到
		return
	}

	// Delete inode from the node.
	// 缩容
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	// 标记需要清理
	n.unbalanced = true
}

// read initializes the node from a page.
// 从页加载节点信息
func (n *node) read(p *page) {
	// 读取页ID
	n.pgid = p.id

	// 读取叶子节点标记
	n.isLeaf = ((p.flags & leafPageFlag) != 0)

	// 读取节点
	n.inodes = make(inodes, int(p.count))
	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]

		if n.isLeaf {
			// 叶子节点，读取标记、key、value
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			// 分支节点，读取页、key
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}

		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// Save first key so we can find the node in the parent when we spill.
	// 设置第一个key
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key

		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// write writes the items onto one or more pages.
// 将node 写入页中
func (n *node) write(p *page) {
	// Initialize page.
	// 写页类型
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	// 写节点数
	if len(n.inodes) >= 0xFFFF {
		// 校验i节点数
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// Stop here if there are no items to write.
	// 空数据判断
	if p.count == 0 {
		return
	}

	// Loop over each item and write it to the page.
	// pass 掉 inode
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]

	// 遍历节点
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// Write the page element.
		// 写页基本信息
		if n.isLeaf {
			// 找偏移量
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))

			// 写基本信息
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			// 找偏移量
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))

			// 写基本信息
			elem.pgid = item.pgid
			elem.ksize = uint32(len(item.key))

			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// If the length of key+value is larger than the max allocation size
		// then we need to reallocate the byte array pointer.
		//
		// See: https://github.com/boltdb/bolt/pull/335
		klen, vlen := len(item.key), len(item.value)
		if len(b) < klen+vlen {
			// 重新申请Buffer
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:]
		}

		// Write data for the element to the end of the page.
		// 写kv
		copy(b[0:], item.key)
		b = b[klen:]
		copy(b[0:], item.value)
		b = b[vlen:]
	}

	// DEBUG ONLY: n.dump()
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
// 分裂
func (n *node) split(pageSize int) []*node {
	var nodes []*node

	// nodes 为分裂结果
	// node 为待分裂节点，每次分裂出a，将a放入nodes中，然后更新node继续
	node := n
	for {
		// Split node into two.
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		if b == nil {
			// 分裂不出了
			break
		}

		// Set node to b so it gets split on the next iteration.
		node = b
	}

	return nodes
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
func (n *node) splitTwo(pageSize int) (*node, *node) {
	// Ignore the split if the page doesn't have at least enough nodes for
	// two pages or if the nodes can fit in a single page.
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		// 子节点数正好，或者页大小合适
		return n, nil
	}

	// Determine the threshold before starting a new node.
	// 计算分裂阈值
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// Determine split position and sizes of the two pages.
	// 找到分裂位置
	splitIndex, _ := n.splitIndex(threshold)

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	if n.parent == nil {
		// 保证有个父节点
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// Create a new node and add it to the parent.
	// 创建兄弟节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	// 分裂数据
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	// 统计打点
	n.bucket.tx.stats.Split++

	return n, next
}

// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	// 保留最小节点数
	// 累加大小至阈值
	for i := 0; i < len(n.inodes) - minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if i >= minKeysPerPage && sz + elsize > threshold {
			// 超过阈值了
			break
		}

		// Add the element size to the total size.
		// 累加大小
		sz += elsize
	}

	return
}

// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled {
		// 已操作过
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	// 先操作子节点
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	// 子节点操作完了
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	// 分裂节点
	var nodes = n.split(tx.db.pageSize)

	// 遍历节点
	for _, node := range nodes {
		// Add node's page to the freelist if it's not new.
		if node.pgid > 0 {
			// 回收页缓存
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// Allocate contiguous space for the node.
		// 申请连续页
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// 写入页
		// Write the node.
		if p.id >= tx.meta.pgid {
			// 异常页id
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		node.pgid = p.id
		node.write(p)
		node.spilled = true

		// Insert into parent inodes.
		if node.parent != nil {
			// 插入到父节点的inode 中
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key

			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// Update the statistics.
		// 统计打点
		tx.stats.Spill++
	}

	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		// 父节点操作
		return n.parent.spill()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	// 校验标识
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Update statistics.
	// 打点
	n.bucket.tx.stats.Rebalance++

	// Ignore if node is above threshold (25%) and has enough keys.
	// 校验阈值
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
	    // 情况良好
		return
	}

	// Root node has special handling.
	if n.parent == nil {
		// 根节点，特殊处理
		// If root node is a branch and only has one node then collapse it.
		if !n.isLeaf && len(n.inodes) == 1 {
			// Move root's child up.
			// 分支转叶子
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Reparent all child nodes being moved.
			// 认爹了
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			// 移除孩子
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// If node has no keys then just remove it.
	if n.numChildren() == 0 {
		// 没有子节点

		// 父节点回收
		n.parent.del(n.key)
		n.parent.removeChild(n)

		// 回收本节点
		delete(n.bucket.nodes, n.pgid)
		n.free()

		// 父节点平衡
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	// 左走或右走
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// If both this node and the target node are too small then merge them.
	if useNextSibling {
		// Reparent all child nodes being moved.
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes from target and remove target.
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// Reparent all child nodes being moved.
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes to target and remove node.
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	// 父节点继续
	n.parent.rebalance()
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
// 删除子节点
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
// 深度复制
func (n *node) dereference() {
	if n.key != nil {
	    // 复制key
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key

		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

    // 操作i节点
	for i := range n.inodes {
		inode := &n.inodes[i]

        // 复制key
		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

        // 复制value
		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	// 操作子节点
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	// 打点
	n.bucket.tx.stats.NodeDeref++
}

// free adds the node's underlying page to the freelist.
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}
*/

// 节点列表
type nodes []*node

func (s nodes) Len() int           { return len(s) }
func (s nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool { return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1 }

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
// i节点
type inode struct {
	// 类型
	flags uint32

	// 页ID
	pgid pgid

	// kv
	key   []byte
	value []byte
}

// i节点列表
type inodes []inode
