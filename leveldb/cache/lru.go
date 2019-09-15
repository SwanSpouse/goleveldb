// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"unsafe"
)

// 双端链表节点
type lruNode struct {
	n          *Node // cache节点，里面包含了数据值
	h          *Handle
	ban        bool     // TODO(done) 这个变量的意思是不再提升这个节点的等级？嗯嗯。被禁用的不会再提升这个节点的升级
	next, prev *lruNode // 前后节点
}

// n是要插入的节点，在at后面插入n
func (n *lruNode) insert(at *lruNode) {
	x := at.next
	at.next = n
	n.prev = at
	n.next = x
	x.prev = n
}

// 删除n节点
func (n *lruNode) remove() {
	if n.prev != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		n.prev = nil
		n.next = nil
	} else {
		// 为啥n不能有prev呢？
		panic("BUG: removing removed node")
	}
}

// Least Recently Used 缓存
// 最近最少使用咱们规定个名词吧，等级。等级越高表明最近使用次数越多，越不容易被回收
// 我理解这个是一个环形链表 recent之后等级越来越低，recent.prev是等级最低的，recent.next等级最高
// 实现起来就是，如果节点被使用一次，就先从列表中删除；然后在recent之后重新插入
type lru struct {
	mu       sync.Mutex // 要有锁
	capacity int        // cache容量
	used     int        // 已使用数量
	recent   lruNode    // TODO recent之后是等级高的，recent之前是等级低的；
}

// 清空lru
func (r *lru) reset() {
	r.recent.next = &r.recent
	r.recent.prev = &r.recent
	r.used = 0
}

// 容量
func (r *lru) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

// 设置lru cache的大小
func (r *lru) SetCapacity(capacity int) {
	var evicted []*lruNode

	r.mu.Lock()
	r.capacity = capacity
	// 如果是缩容的话，需要删除部分多余的数据
	for r.used > r.capacity {
		// 获取recent前面的节点
		rn := r.recent.prev
		if rn == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		// 删除节点
		rn.remove()
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		// 将要删除的数据塞到这里
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()
	// 依次执行节点的release方法
	for _, rn := range evicted {
		rn.h.Release()
	}
}

// 推广Node，也就是node在近期被使用了，提升它在缓存中的等级，防止被删除
func (r *lru) Promote(n *Node) {
	var evicted []*lruNode

	r.mu.Lock()
	// 如果node的CacheData为空
	if n.CacheData == nil {
		// 如果n的size足够放到lru中
		if n.Size() <= r.capacity {
			// 构造lruNode节点
			rn := &lruNode{n: n, h: n.GetHandle()}
			// 将这个节点插入到recent之后
			rn.insert(&r.recent)
			// 将rn赋值给CacheData
			n.CacheData = unsafe.Pointer(rn)
			// used大小+Size
			r.used += n.Size()
			// 如果used > capacity就要往外驱逐node了
			for r.used > r.capacity {
				// recent node之前的节点都是等级比较低的。
				rn := r.recent.prev
				if rn == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				// 清空数据
				rn.remove()
				rn.n.CacheData = nil
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	} else {
		rn := (*lruNode)(n.CacheData)
		// 被禁了就没有办法提升等级了
		if !rn.ban {
			rn.remove()
			rn.insert(&r.recent)
		}
	}
	// TODO limingji 这个文件里面的所有解锁操作都没有在defer中进行
	r.mu.Unlock()
	// 依次释放要驱逐出cache的node
	for _, rn := range evicted {
		rn.h.Release()
	}
}

// 禁用某个节点，不允许节点进行promote
func (r *lru) Ban(n *Node) {
	r.mu.Lock()
	// TODO limingji n.CacheData == nil 说明node没有其对应的lruNode ????
	if n.CacheData == nil {
		// 这个是为啥呢？
		n.CacheData = unsafe.Pointer(&lruNode{n: n, ban: true})
	} else {
		// 数据不为空，如果原来没有被禁，则直接从lru中挪出去
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.ban = true
			r.used -= rn.n.Size()
			r.mu.Unlock()

			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

// 释放node数据
func (r *lru) Evict(n *Node) {
	r.mu.Lock()
	rn := (*lruNode)(n.CacheData)
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

// 根据namespace 释放数据
func (r *lru) EvictNS(ns uint64) {
	var evicted []*lruNode

	r.mu.Lock()
	for e := r.recent.prev; e != &r.recent; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.used -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

// 释放所有数据
func (r *lru) EvictAll() {
	r.mu.Lock()
	back := r.recent.prev
	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.n.CacheData = nil
	}
	r.reset()
	r.mu.Unlock()

	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.h.Release()
	}
}

// 啥也没做，只是为了实现接口
func (r *lru) Close() error {
	return nil
}

// NewLRU create a new LRU-cache.
// 创建一个LRU Cache
func NewLRU(capacity int) Cacher {
	r := &lru{capacity: capacity}
	r.reset()
	return r
}
