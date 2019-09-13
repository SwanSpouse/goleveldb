// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
package memdb

import (
	"math/rand"
	"sync"

	"github.com/SwanSpouse/goleveldb/leveldb/comparer"
	"github.com/SwanSpouse/goleveldb/leveldb/errors"
	"github.com/SwanSpouse/goleveldb/leveldb/iterator"
	"github.com/SwanSpouse/goleveldb/leveldb/util"
)

// Common errors.
var (
	ErrNotFound     = errors.ErrNotFound                             // 未找到指定key
	ErrIterReleased = errors.New("leveldb/memdb: iterator released") // 迭代器已经被释放
)

const tMaxHeight = 12

type dbIter struct {
	util.BasicReleaser
	p          *DB         // iterator包含db，不是db包含iterator
	slice      *util.Range // key range
	node       int         // 当前节点
	forward    bool
	key, value []byte
	err        error
}

func (i *dbIter) fill(checkStart, checkLimit bool) bool {
	if i.node != 0 {
		n := i.p.nodeData[i.node]
		m := n + i.p.nodeData[i.node+nKey]
		i.key = i.p.kvData[n:m]
		if i.slice != nil {
			switch {
			case checkLimit && i.slice.Limit != nil && i.p.cmp.Compare(i.key, i.slice.Limit) >= 0:
				fallthrough
			case checkStart && i.slice.Start != nil && i.p.cmp.Compare(i.key, i.slice.Start) < 0:
				i.node = 0
				goto bail
			}
		}
		i.value = i.p.kvData[m : m+i.p.nodeData[i.node+nVal]]
		return true
	}
bail:
	i.key = nil
	i.value = nil
	return false
}

func (i *dbIter) Valid() bool {
	return i.node != 0
}

func (i *dbIter) First() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil {
		i.node, _ = i.p.findGE(i.slice.Start, false)
	} else {
		// 更新当前节点
		i.node = i.p.nodeData[nNext]
	}
	return i.fill(false, true)
}

func (i *dbIter) Last() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Limit != nil {
		i.node = i.p.findLT(i.slice.Limit)
	} else {
		i.node = i.p.findLast()
	}
	return i.fill(true, false)
}

func (i *dbIter) Seek(key []byte) bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil && i.p.cmp.Compare(key, i.slice.Start) < 0 {
		key = i.slice.Start
	}
	i.node, _ = i.p.findGE(key, false)
	return i.fill(false, true)
}

func (i *dbIter) Next() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if !i.forward {
			return i.First()
		}
		return false
	}
	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.nodeData[i.node+nNext]
	return i.fill(false, true)
}

func (i *dbIter) Prev() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if i.forward {
			return i.Last()
		}
		return false
	}
	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.findLT(i.key)
	return i.fill(true, false)
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Error() error { return i.err }

func (i *dbIter) Release() {
	if !i.Released() {
		i.p = nil
		i.node = 0
		i.key = nil
		i.value = nil
		i.BasicReleaser.Release()
	}
}

const (
	nKV     = iota // 0 kv offset
	nKey           // 1 key length
	nVal           // 2 value length
	nHeight        // height
	nNext          // next nodes
)

// DB is an in-memory key/value database.
// 其实是个跳跃表
// TODO(done) 这是一个拍扁了的跳跃表？？？
// 数据都存储在kvData里面，然后所有的索引也就是跳跃表该有的信息都存储在nodeData里面。
type DB struct {
	cmp    comparer.BasicComparer // 比较器
	rnd    *rand.Rand             // 用于产生层高的随机因子
	mu     sync.RWMutex           // 内存中的数据是要加锁的
	kvData []byte                 // TODO(done) kvData 单纯只key value 数据？和下面的nodeData有啥区别 存储数据
	// Node data:
	// [0]         : KV offset    // 这个结构是嘎哈的没有理解 offset是索引，用来标记数据在kvData中的位置
	// [1]         : Key length   // key长度
	// [2]         : Value length // value长度
	// [3]         : Height       // 当前节点的层高
	// [3..height] : Next nodes   // 这个标识写的具有误导性啊，3..height，如果当前的高度是2呢？ 应该写成 3..3+height
	nodeData  []int           // 跳跃表结构
	prevNode  [tMaxHeight]int // TODO prevNode是各个层，比node大的节点都指向了node 这个问题脑袋有点儿乱了，明天看吧。
	maxHeight int             // 当前跳跃表中的最高height
	n         int             // db中key value对的数量
	kvSize    int             // TODO(done) kvSize key value的size??? kvData里面有效key value的长度 <= len(kvData)
}

// 随机产生一个高度，越高层级，概率越低；它这个是25%的几率增高一层
func (p *DB) randHeight() (h int) {
	const branching = 4
	h = 1
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return
}

// 必须用锁，因为他们用了共享的prevNdoe结构
// Must hold RW-lock if prev == true, as it use shared prevNode slice.
// 找到 >= key的位置
// 返回的是在跳跃表中的位置也就是offset对应的索引位置
func (p *DB) findGE(key []byte, prev bool) (int, bool) {
	// node = 0 TODO(done) 是说从头开始找？跳跃表有个表头，从表头开始找
	node := 0
	// 从头节点的最高层开始往后找
	h := p.maxHeight - 1
	for {
		// 获取h层next节点的位置
		next := p.nodeData[node+nNext+h]
		// 默认next key值大于 给定的key值
		cmp := 1
		// 说明后面有节点
		if next != 0 {
			// o就是offset，找到索引之后，根据索引去kvData里面去取值
			o := p.nodeData[next]
			// 和输入的key值进行比较
			// p.nodeData[next+nKey]这个就是key的长度,
			// 所以o的位置 <-> o+p.nodeData[next+nKey]这个位置，把kvData里面的这一段取出来，就是key值
			cmp = p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key)
		}
		// 如果next比给定的key值小，说明要找的节点还在后面
		if cmp < 0 {
			// Keep searching in this list
			node = next
		} else {
			// 如果next比给定的key值大，要降一层
			if prev {
				// 在查找的过程中，把prevNode给赋值了。。。
				// prevNode是各个层，比node大的节点都指向了node
				p.prevNode[h] = node
			} else if cmp == 0 {
				// 如果是和给定的key值相等，直接放回next和"找到元素"标志
				return next, true
			}
			// 如果已经是最底层了，那么直接返回next 和 cmp == 0
			if h == 0 {
				return next, cmp == 0
			}
			// 如果要是向后找的话；说明这个next比给定的值大了，要层高降一级来查找
			h--
		}
	}
}

// 找到比给定key小的 key的位置
func (p *DB) findLT(key []byte) int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		o := p.nodeData[next]
		// 如果当前层没有next，或者next的key值大于等于给定key；要降低层高来查找
		if next == 0 || p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key) >= 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// 找到跳跃表中的最后一个节点
func (p *DB) findLast() int {
	// 这个也是从表头开始向后一直遍历。能走高层向后跑，就走高层向后跑；
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
func (p *DB) Put(key []byte, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 首先看>= key的是否存在，如果找到了，则是进行覆盖，但是没有覆盖原来的key value值，只是覆盖了跳跃表里面的索引
	// TODO prev是是否修改prev指针的意思??
	if node, exact := p.findGE(key, true); exact {
		// offset 是kv数据的开始索引，所以要指到kvData的最后的位置，也就是len(p.kvData)
		kvOffset := len(p.kvData)
		// 然后往里面添加 key和value的值
		p.kvData = append(p.kvData, key...)
		p.kvData = append(p.kvData, value...)
		// 覆盖索引
		p.nodeData[node] = kvOffset
		// 这里key的长度不用覆盖，因为key和原来的key相同，只是value变了
		// 原来value的长度
		m := p.nodeData[node+nVal]
		// value的长度覆盖
		p.nodeData[node+nVal] = len(value)
		// 重新计算长度
		p.kvSize += len(value) - m
		return nil
	}

	// 先随机出一个高度值
	h := p.randHeight()
	if h > p.maxHeight {
		// 从当前的高度 到 h这么高，prevNode都赋值成0，因为它现在是最高的；没有prev节点
		for i := p.maxHeight; i < h; i++ {
			p.prevNode[i] = 0
		}
		// 如果随机出来的高度大于现有的所有高度，则将现有的最高高度进行更新
		p.maxHeight = h
	}

	// kvData就是一个only append的操作，只有append 数据覆盖了也不会进行删除
	// 计算索引位置，kvData 直接把key value值拼到后面
	kvOffset := len(p.kvData)
	p.kvData = append(p.kvData, key...)
	p.kvData = append(p.kvData, value...)

	// Node 新节点的位置
	node := len(p.nodeData)
	// 依次写入相关数据 kvOffset ,len key ,len value ,h
	p.nodeData = append(p.nodeData, kvOffset, len(key), len(value), h)

	// 依次计算新节点的next和prev节点
	// 根据上面算的prevNode，来计算node的next
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i
		p.nodeData = append(p.nodeData, p.nodeData[m])
		p.nodeData[m] = node
	}
	// 统计有效kv数据长度
	p.kvSize += len(key) + len(value)
	// n是当前memdb中key-value对的个数
	p.n++
	return nil
}

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (p *DB) Delete(key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, exact := p.findGE(key, true)
	if !exact {
		return ErrNotFound
	}

	h := p.nodeData[node+nHeight]
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i
		p.nodeData[m] = p.nodeData[p.nodeData[m]+nNext+i]
	}

	p.kvSize -= p.nodeData[node+nKey] + p.nodeData[node+nVal]
	p.n--
	return nil
}

// Contains returns true if the given key are in the DB.
//
// It is safe to modify the contents of the arguments after Contains returns.
// 这个最好理解，判断一个key在db中有没有
func (p *DB) Contains(key []byte) bool {
	p.mu.RLock()
	_, exact := p.findGE(key, false)
	p.mu.RUnlock()
	return exact
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
// 这个和上面差不多
func (p *DB) Get(key []byte) (value []byte, err error) {
	p.mu.RLock()
	if node, exact := p.findGE(key, false); exact {
		o := p.nodeData[node] + p.nodeData[node+nKey]
		value = p.kvData[o : o+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
// 一样的。和上面
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	if node, _ := p.findGE(key, false); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// NewIterator returns an iterator of the DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. However, the resultant key/value pairs are not guaranteed
// to be a consistent snapshot of the DB at a particular point in time.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (p *DB) NewIterator(slice *util.Range) iterator.Iterator {
	return &dbIter{p: p, slice: slice}
}

// Capacity returns keys/values buffer capacity.
func (p *DB) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData)
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
// 被删除的节点依旧占用buffer
func (p *DB) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.kvSize
}

// Free returns keys/values free buffer before need to grow.
// 还能容纳多少bytes
// 有歧义啊，Free明显是一个动词，FreeSize这种应该
func (p *DB) Free() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData) - len(p.kvData)
}

// Len returns the number of entries in the DB.
// memdb中元素的个数
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

// Reset resets the DB to initial empty state. Allows reuse the buffer.
// 清空memdb
func (p *DB) Reset() {
	p.mu.Lock()
	p.rnd = rand.New(rand.NewSource(0xdeadbeef))
	p.maxHeight = 1
	p.n = 0
	p.kvSize = 0
	p.kvData = p.kvData[:0]
	p.nodeData = p.nodeData[:nNext+tMaxHeight]
	p.nodeData[nKV] = 0
	p.nodeData[nKey] = 0
	p.nodeData[nVal] = 0
	p.nodeData[nHeight] = tMaxHeight
	for n := 0; n < tMaxHeight; n++ {
		p.nodeData[nNext+n] = 0 // p.nodeData[0+nNext+n] = 0 这样更好理解一些表示头结点的后面没有元素了
		p.prevNode[n] = 0
	}
	p.mu.Unlock()
}

// New creates a new initialized in-memory key/value DB. The capacity
// is the initial key/value buffer capacity. The capacity is advisory,
// not enforced.
//
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
//
// The returned DB instance is safe for concurrent use.
// 并发安全，只读，TODO limingji 删除会删除一个节点，但是不会回收kv buffer中的内存？
// 这个capacity是建议性的，不是强制的。
func New(cmp comparer.BasicComparer, capacity int) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)), // TODO limingji 这里的随机种子是一样的是吗？
		maxHeight: 1,                                    // TODO(done) 最高层是1？ 当前最高层
		kvData:    make([]byte, 0, capacity),            // 这个用来存储数据
		nodeData:  make([]int, 4+tMaxHeight),            // TODO(done) 跳跃表有个头
	}
	// TODO(done) 这个是头结点吗? 头结点有12层。
	p.nodeData[nHeight] = tMaxHeight
	return p
}
