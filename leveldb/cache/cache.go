// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package cache provides interface and implementation of a cache algorithms.
package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/SwanSpouse/goleveldb/leveldb/util"
)

// Cacher provides interface to implements a caching functionality.
// An implementation must be safe for concurrent use.
// 首先是Cacher接口的定义，现在来看其实现只有lru 一种
type Cacher interface {
	// Capacity returns cache capacity.
	Capacity() int

	// SetCapacity sets cache capacity.
	SetCapacity(capacity int)

	// Promote promotes the 'cache node'.
	Promote(n *Node)

	// Ban evicts the 'cache node' and prevent subsequent 'promote'.
	Ban(n *Node)

	// Evict evicts the 'cache node'.
	Evict(n *Node)

	// EvictNS evicts 'cache node' with the given namespace.
	EvictNS(ns uint64)

	// EvictAll evicts all 'cache node'.
	EvictAll()

	// Close closes the 'cache tree'
	Close() error
}

// Value is a 'cacheable object'. It may implements util.Releaser, if
// so the the Release method will be called once object is released.
type Value interface{}

// NamespaceGetter provides convenient wrapper for namespace.
type NamespaceGetter struct {
	Cache *Cache
	NS    uint64
}

// Get simply calls Cache.Get() method.
// 从特定的命名空间中获取变量
func (g *NamespaceGetter) Get(key uint64, setFunc func() (size int, value Value)) *Handle {
	return g.Cache.Get(g.NS, key, setFunc)
}

// The hash tables implementation is based on:
// "Dynamic-Sized Nonblocking Hash Tables", by Yujie Liu,
// Kunlong Zhang, and Michael Spear.
// ACM Symposium on Principles of Distributed Computing, Jul 2014.

const (
	mInitialSize           = 1 << 4 // 16
	mOverflowThreshold     = 1 << 5 // 32
	mOverflowGrowThreshold = 1 << 7 // 256
)

type mBucket struct {
	mu     sync.Mutex
	node   []*Node // 一组Node
	frozen bool
}

// 冻结Bucket中的所有Node
func (b *mBucket) freeze() []*Node {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.frozen {
		b.frozen = true
	}
	return b.node
}

// noset为空表示get失败的时候，不会创建新值进去
func (b *mBucket) get(r *Cache, h *mNode, hash uint32, ns, key uint64, noset bool) (done, added bool, n *Node) {
	b.mu.Lock()
	// 被冻结的Bucket不允许进行操作
	if b.frozen {
		b.mu.Unlock()
		return
	}

	// Scan the node.
	// 依次遍历bucket下的所有节点
	for _, n := range b.node {
		// 找到对应的key
		if n.hash == hash && n.ns == ns && n.key == key {
			atomic.AddInt32(&n.ref, 1)
			b.mu.Unlock()
			return true, false, n
		}
	}
	// Get only. 如果没有找到直接返回
	if noset {
		b.mu.Unlock()
		return true, false, nil
	}

	// Create node. 没找到仍然创建Node
	n = &Node{
		r:    r,
		hash: hash,
		ns:   ns,
		key:  key,
		ref:  1,
	}
	// Add node to bucket.
	b.node = append(b.node, n)
	// 重新计算node数组的长度
	bLen := len(b.node)
	b.mu.Unlock()

	// Update counter.
	grow := atomic.AddInt32(&r.nodes, 1) >= h.growThreshold
	if bLen > mOverflowThreshold {
		grow = grow || atomic.AddInt32(&h.overflow, 1) >= mOverflowGrowThreshold
	}

	// 对bucket进行扩容
	// Grow.
	if grow && atomic.CompareAndSwapInt32(&h.resizeInProgess, 0, 1) {
		// 容量翻倍
		nhLen := len(h.buckets) << 1
		nh := &mNode{
			buckets:         make([]unsafe.Pointer, nhLen),
			mask:            uint32(nhLen) - 1,                 // 2的n次方-1；相当于各个位都是1
			pred:            unsafe.Pointer(h),                 // 老的mNode
			growThreshold:   int32(nhLen * mOverflowThreshold), // 重新计算扩容阈值
			shrinkThreshold: int32(nhLen >> 1),                 // 缩容阈值
		}
		ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
		if !ok {
			panic("BUG: failed swapping head")
		}
		// 并发重新初始化buckets
		go nh.initBuckets()
	}

	return true, true, n
}

func (b *mBucket) delete(r *Cache, h *mNode, hash uint32, ns, key uint64) (done, deleted bool) {
	b.mu.Lock()

	if b.frozen {
		b.mu.Unlock()
		return
	}

	// Scan the node.
	var (
		n    *Node
		bLen int
	)
	for i := range b.node {
		n = b.node[i]
		if n.ns == ns && n.key == key {
			if atomic.LoadInt32(&n.ref) == 0 {
				deleted = true

				// Call releaser.
				if n.value != nil {
					if r, ok := n.value.(util.Releaser); ok {
						r.Release()
					}
					n.value = nil
				}

				// Remove node from bucket.
				b.node = append(b.node[:i], b.node[i+1:]...)
				bLen = len(b.node)
			}
			break
		}
	}
	b.mu.Unlock()

	if deleted {
		// Call OnDel.
		for _, f := range n.onDel {
			f()
		}

		// Update counter.
		atomic.AddInt32(&r.size, int32(n.size)*-1)
		shrink := atomic.AddInt32(&r.nodes, -1) < h.shrinkThreshold
		if bLen >= mOverflowThreshold {
			atomic.AddInt32(&h.overflow, -1)
		}

		// Shrink.
		if shrink && len(h.buckets) > mInitialSize && atomic.CompareAndSwapInt32(&h.resizeInProgess, 0, 1) {
			nhLen := len(h.buckets) >> 1
			// 搞一个新的mNode
			nh := &mNode{
				buckets:         make([]unsafe.Pointer, nhLen),
				mask:            uint32(nhLen) - 1,
				pred:            unsafe.Pointer(h),
				growThreshold:   int32(nhLen * mOverflowThreshold),
				shrinkThreshold: int32(nhLen >> 1),
			}
			// 把新的mNode复制到mHead里面
			ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
			if !ok {
				panic("BUG: failed swapping head")
			}
			go nh.initBuckets()
		}
	}

	return true, deleted
}

// mNode结构，用于存放Node，根据hash值将data分散到不同的bucket中
type mNode struct {
	buckets         []unsafe.Pointer // []*mBucket 通常是2的幂次
	mask            uint32           // 掩码，2的幂次-1，i&mask用来计算在哪个bucket中
	pred            unsafe.Pointer   // *mNode 表明扩容、缩容前的老的mNode
	resizeInProgess int32            // 正在扩容的标识

	overflow        int32 // 溢出的阈值
	growThreshold   int32 // 扩容的阈值
	shrinkThreshold int32 // 缩容的阈值
}

// 初始化Bucket
func (n *mNode) initBucket(i uint32) *mBucket {
	// 不为空则直接返回，
	if b := (*mBucket)(atomic.LoadPointer(&n.buckets[i])); b != nil {
		return b
	}
	// 要从老的mNode迁移到新的mNode
	p := (*mNode)(atomic.LoadPointer(&n.pred))
	if p != nil {
		var node []*Node
		if n.mask > p.mask {
			// Grow. 新的mask大于老的，这表明是扩大容量
			pb := (*mBucket)(atomic.LoadPointer(&p.buckets[i&p.mask]))
			// 如果为空则创建一个
			if pb == nil {
				pb = p.initBucket(i & p.mask)
			}
			// 冻结
			m := pb.freeze()
			// Split nodes. 将节点从老的迁移到新的中
			for _, x := range m {
				// 假如老的大小为4，扩容后的为8；则老的buckets[3]中的数据只能在新buckets中
				// buckets[3]和buckets[7]中；
				if x.hash&n.mask == i {
					node = append(node, x)
				}
			}
		} else {
			// Shrink. 缩容
			pb0 := (*mBucket)(atomic.LoadPointer(&p.buckets[i]))
			if pb0 == nil {
				pb0 = p.initBucket(i)
			}
			pb1 := (*mBucket)(atomic.LoadPointer(&p.buckets[i+uint32(len(n.buckets))]))
			if pb1 == nil {
				pb1 = p.initBucket(i + uint32(len(n.buckets)))
			}
			// 和上面扩容时候的概念一样，老的3号和7号都会缩到3号中；
			m0 := pb0.freeze()
			m1 := pb1.freeze()
			// Merge nodes.
			node = make([]*Node, 0, len(m0)+len(m1))
			node = append(node, m0...)
			node = append(node, m1...)
			// 把老的数据合并到新的里面
		}
		// 这个是新的bucket
		b := &mBucket{node: node}
		// 迁移过去
		if atomic.CompareAndSwapPointer(&n.buckets[i], nil, unsafe.Pointer(b)) {
			if len(node) > mOverflowThreshold {
				atomic.AddInt32(&n.overflow, int32(len(node)-mOverflowThreshold))
			}
			return b
		}
	}

	return (*mBucket)(atomic.LoadPointer(&n.buckets[i]))
}

func (n *mNode) initBuckets() {
	for i := range n.buckets {
		n.initBucket(uint32(i))
	}
	atomic.StorePointer(&n.pred, nil)
}

// Cache is a 'cache map'.
// TODO limingji mHead是一个链表吗？ 感觉Cache里面就这么一个节点呢？
type Cache struct {
	mu     sync.RWMutex
	mHead  unsafe.Pointer // *mNode 这个才是存储数据的
	nodes  int32          // cache中 node的个数
	size   int32
	cacher Cacher
	closed bool
}

// NewCache creates a new 'cache map'. The cacher is optional and
// may be nil.
func NewCache(cacher Cacher) *Cache {
	// 创建一个mNode
	h := &mNode{
		buckets:         make([]unsafe.Pointer, mInitialSize), // 初始化buckets
		mask:            mInitialSize - 1,
		growThreshold:   int32(mInitialSize * mOverflowThreshold),
		shrinkThreshold: 0,
	}
	// 依次创建空的bucket
	for i := range h.buckets {
		h.buckets[i] = unsafe.Pointer(&mBucket{})
	}
	// Cache其实就是一个mNode
	r := &Cache{
		mHead:  unsafe.Pointer(h),
		cacher: cacher,
	}
	return r
}

// 根据给定的hash值，找到相应的bucket
func (r *Cache) getBucket(hash uint32) (*mNode, *mBucket) {
	// 找到cache
	h := (*mNode)(atomic.LoadPointer(&r.mHead))
	i := hash & h.mask // 这一个步骤相当于取mask位
	// 然后把里面的bucket拿出来
	b := (*mBucket)(atomic.LoadPointer(&h.buckets[i]))
	// 如果为空，则进行初始化
	if b == nil {
		b = h.initBucket(i)
	}
	return h, b
}

func (r *Cache) delete(n *Node) bool {
	// 如果没有删除成功则反复删除
	for {
		// 首先根据node的hash值找到对应的mNode和mBucket
		h, b := r.getBucket(n.hash)
		// 删除
		done, deleted := b.delete(r, h, n.hash, n.ns, n.key)
		if done {
			return deleted
		}
	}
}

// Nodes returns number of 'cache node' in the map.
// 获取Cache中的节点个数
func (r *Cache) Nodes() int {
	return int(atomic.LoadInt32(&r.nodes))
}

// Size returns sums of 'cache node' size in the map.
func (r *Cache) Size() int {
	return int(atomic.LoadInt32(&r.size))
}

// Capacity returns cache capacity.
func (r *Cache) Capacity() int {
	if r.cacher == nil {
		return 0
	}
	return r.cacher.Capacity()
}

// SetCapacity sets cache capacity.
func (r *Cache) SetCapacity(capacity int) {
	if r.cacher != nil {
		r.cacher.SetCapacity(capacity)
	}
}

// Get gets 'cache node' with the given namespace and key.
// If cache node is not found and setFunc is not nil, Get will atomically creates
// the 'cache node' by calling setFunc. Otherwise Get will returns nil.
//
// The returned 'cache handle' should be released after use by calling Release
// method.
// setFunc是当没有找到的时候创建一个
// 所以这个Get方法也有Set的作用
func (r *Cache) Get(ns, key uint64, setFunc func() (size int, value Value)) *Handle {
	// 这里是一个读锁
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil
	}
	// 根据ns key和seed生成hash值
	hash := murmur32(ns, key, 0xf00)
	for {
		// 根据hash值找到特定的bucket
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, setFunc == nil)
		if done {
			if n != nil {
				n.mu.Lock()
				// 没有找到对应的值
				if n.value == nil {
					// 没有找到对应值，并且setFunc == nil则表示不需要新建
					if setFunc == nil {
						n.mu.Unlock()
						n.unref()
						return nil
					}
					// 否则会通过setFunc来创建一个value
					n.size, n.value = setFunc()
					if n.value == nil {
						n.size = 0
						n.mu.Unlock()
						n.unref()
						return nil
					}
					atomic.AddInt32(&r.size, int32(n.size))
				}
				n.mu.Unlock()
				if r.cacher != nil {
					r.cacher.Promote(n)
				}
				return &Handle{unsafe.Pointer(n)}
			}

			break
		}
	}
	return nil
}

// Delete removes and ban 'cache node' with the given namespace and key.
// A banned 'cache node' will never inserted into the 'cache tree'. Ban
// only attributed to the particular 'cache node', so when a 'cache node'
// is recreated it will not be banned.
//
// If onDel is not nil, then it will be executed if such 'cache node'
// doesn't exist or once the 'cache node' is released.
//
// Delete return true is such 'cache node' exist.
func (r *Cache) Delete(ns, key uint64, onDel func()) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}
	// 计算hash值
	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, true)
		if done {
			if n != nil {
				if onDel != nil {
					n.mu.Lock()
					n.onDel = append(n.onDel, onDel)
					n.mu.Unlock()
				}
				if r.cacher != nil {
					r.cacher.Ban(n)
				}
				n.unref()
				return true
			}

			break
		}
	}
	// onDel 不为空则执行
	if onDel != nil {
		onDel()
	}

	return false
}

// Evict evicts 'cache node' with the given namespace and key. This will
// simply call Cacher.Evict.
//
// Evict return true is such 'cache node' exist.
func (r *Cache) Evict(ns, key uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, true)
		if done {
			if n != nil {
				if r.cacher != nil {
					r.cacher.Evict(n)
				}
				n.unref()
				return true
			}

			break
		}
	}

	return false
}

// EvictNS evicts 'cache node' with the given namespace. This will
// simply call Cacher.EvictNS.
func (r *Cache) EvictNS(ns uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictNS(ns)
	}
}

// EvictAll evicts all 'cache node'. This will simply call Cacher.EvictAll.
func (r *Cache) EvictAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictAll()
	}
}

// Close closes the 'cache map' and forcefully releases all 'cache node'.
func (r *Cache) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true

		h := (*mNode)(r.mHead)
		h.initBuckets()

		for i := range h.buckets {
			b := (*mBucket)(h.buckets[i])
			for _, n := range b.node {
				// Call releaser.
				if n.value != nil {
					if r, ok := n.value.(util.Releaser); ok {
						r.Release()
					}
					n.value = nil
				}

				// Call OnDel.
				for _, f := range n.onDel {
					f()
				}
				n.onDel = nil
			}
		}
	}
	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CloseWeak closes the 'cache map' and evict all 'cache node' from cacher, but
// unlike Close it doesn't forcefully releases 'cache node'.
func (r *Cache) CloseWeak() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
	}
	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		r.cacher.EvictAll()
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Node is a 'cache node'.
type Node struct {
	r *Cache

	hash    uint32 // hash值
	ns, key uint64 // 命名空间和key TODO limingji 为啥key是一个uint64

	mu    sync.Mutex
	size  int
	value Value

	ref   int32 // 引用计数法
	onDel []func()

	// 这是个抽象结构，不能依赖子类，所以只能在这里定义一个CacheData，
	// 我理解和interface{}一样，只不过这个规定了只允许是指针
	/*
		关于unsafe.Pointer的4个规则:
		1. 任何指针都可以转换为unsafe.Pointer
		2. unsafe.Pointer可以转换为任何指针
		3. uintptr可以转换为unsafe.Pointer
		4. unsafe.Pointer可以转换为uintptr
	*/
	CacheData unsafe.Pointer // e.g. *lruNode
}

// NS returns this 'cache node' namespace.
func (n *Node) NS() uint64 {
	return n.ns
}

// Key returns this 'cache node' key.
func (n *Node) Key() uint64 {
	return n.key
}

// Size returns this 'cache node' size.
func (n *Node) Size() int {
	return n.size
}

// Value returns this 'cache node' value.
func (n *Node) Value() Value {
	return n.value
}

// Ref returns this 'cache node' ref counter.
func (n *Node) Ref() int32 {
	return atomic.LoadInt32(&n.ref)
}

// GetHandle returns an handle for this 'cache node'.
func (n *Node) GetHandle() *Handle {
	if atomic.AddInt32(&n.ref, 1) <= 1 {
		panic("BUG: Node.GetHandle on zero ref")
	}
	return &Handle{unsafe.Pointer(n)}
}

func (n *Node) unref() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.delete(n)
	}
}

func (n *Node) unrefLocked() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.mu.RLock()
		if !n.r.closed {
			n.r.delete(n)
		}
		n.r.mu.RUnlock()
	}
}

// Handle is a 'cache handle' of a 'cache node'.
type Handle struct {
	n unsafe.Pointer // *Node 这里n需要是一个unsafe.Pointer类型
}

// Value returns the value of the 'cache node'.
func (h *Handle) Value() Value {
	n := (*Node)(atomic.LoadPointer(&h.n))
	if n != nil {
		return n.value
	}
	return nil
}

// Release releases this 'cache handle'.
// It is safe to call release multiple times.
func (h *Handle) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr != nil && atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*Node)(nPtr)
		n.unrefLocked()
	}
}

// 生成hash值的方法
func murmur32(ns, key uint64, seed uint32) uint32 {
	const (
		m = uint32(0x5bd1e995)
		r = 24
	)

	k1 := uint32(ns >> 32)
	k2 := uint32(ns)
	k3 := uint32(key >> 32)
	k4 := uint32(key)

	k1 *= m
	k1 ^= k1 >> r
	k1 *= m

	k2 *= m
	k2 ^= k2 >> r
	k2 *= m

	k3 *= m
	k3 ^= k3 >> r
	k3 *= m

	k4 *= m
	k4 ^= k4 >> r
	k4 *= m

	h := seed

	h *= m
	h ^= k1
	h *= m
	h ^= k2
	h *= m
	h ^= k3
	h *= m
	h ^= k4

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}
