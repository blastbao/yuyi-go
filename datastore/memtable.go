// Copyright 2015 The yuyi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"bytes"
	"github.com/blastbao/yuyi-go/datastore/chunk"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

type MemTable struct {
	// skipList the skip list to holds entries
	skipList *SkipList

	// firstSeq the first entry's sequence
	firstSeq uint64

	// lastSeq the last entry's sequence
	lastSeq uint64

	// lastWalAddr
	lastWalAddr chunk.Address

	// sealed if the memory table is sealed(not allowed to edit)
	sealed bool

	// capacity current capacity of the memory table
	capacity int
}

func NewMemTable() *MemTable {
	skipList := NewSkipList()
	return &MemTable{
		skipList: skipList,
		sealed:   false,
		capacity: 0,
	}
}

func (mt *MemTable) Put(entry *KVEntry) error {
	err := mt.skipList.Put(entry)
	if err != nil {
		// failed to put entry to this memory table
	}
	mt.capacity = mt.capacity + entry.size()
	if mt.lastSeq == 0 {
		mt.firstSeq = entry.Seq
	}
	mt.lastSeq = entry.Seq
	return nil
}

func (mt *MemTable) Has(key Key, seq uint64) bool {
	return mt.skipList.Get(key, seq) != nil
}

func (mt *MemTable) Get(key Key, seq uint64) *TableValue {
	return mt.skipList.Get(key, seq)
}

func (mt *MemTable) List(start Key, end Key, seq uint64) *listIter {
	return mt.newMemTableIter(start, end, seq)
}

func (mt *MemTable) newMemTableIter(start Key, end Key, seq uint64) *listIter {
	return newListIter(mt.skipList, start, end, seq)
}

type node struct {
	key   Key
	value *TableValue
	seq   uint64

	next *node
}

func (node *node) casNext(old *node, new *node) bool {
	var unsafePtr = (*unsafe.Pointer)(unsafe.Pointer(&node.next))
	return atomic.CompareAndSwapPointer(unsafePtr, unsafe.Pointer(old), unsafe.Pointer(new))
}

type index struct {
	node  *node
	down  *index
	right *index
}

func (index *index) casRight(old *index, new *index) bool {
	var unsafePtr = (*unsafe.Pointer)(unsafe.Pointer(&index.right))
	return atomic.CompareAndSwapPointer(unsafePtr, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (index *index) link(succ *index, newSucc *index) bool {
	n := index.node
	newSucc.right = succ
	return n != nil && index.casRight(succ, newSucc)
}

type head struct {
	index *index
	level int
}

type SkipList struct {
	head *head
}

func NewSkipList() *SkipList {
	n := &node{
		seq: 0,
	}
	i := &index{
		node: n,
	}
	h := &head{
		index: i,
		level: 1,
	}
	return &SkipList{
		head: h,
	}
}

func (skipList *SkipList) casHead(old *head, new *head) bool {
	var unsafePtr = (*unsafe.Pointer)(unsafe.Pointer(&skipList.head))
	return atomic.CompareAndSwapPointer(unsafePtr, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (skipList *SkipList) Put(entry *KVEntry) error {
	return skipList.doPut(entry)
}

func (skipList *SkipList) Get(key Key, seq uint64) *TableValue {
	return skipList.doGet(key, seq)
}

func (skipList *SkipList) doPut(entry *KVEntry) error {
	var added *node
outer:
	for {
		pre := skipList.findPredecessor(entry.Key, entry.Seq)
		suf := pre.next
		for {
			if suf != nil {
				if pre.next != suf {
					break
				}
				c := compareKeyAndSeq(entry.Key, entry.Seq, suf.key, suf.seq)
				if c > 0 {
					pre = suf
					suf = suf.next
					continue
				}
				if c == 0 {
					// the compare result should never be 0 due to seq is increment automic
				}
			}
			added = &node{
				key:   entry.Key,
				value: &entry.TableValue,
				seq:   entry.Seq,
				next:  suf,
			}
			if !pre.casNext(suf, added) {
				break
			}
			break outer
		}
	}
	rnd := rand.Uint32()
	if (rnd & 0x80000001) == 0 {
		level := 1
		for {
			rnd = rnd >> 1
			if rnd&1 != 0 {
				level++
			} else {
				break
			}
		}
		var idx *index
		h := skipList.head
		max := h.level
		if level <= max {
			for i := 1; i <= level; i++ {
				idx = &index{
					node:  added,
					down:  idx,
					right: nil,
				}
			}
		} else {
			level = max + 1
			idxs := make([]*index, level+1, level+1)
			for i := 1; i <= level; i++ {
				idx = &index{
					node:  added,
					down:  idx,
					right: nil,
				}
				idxs[i] = idx
			}
			for {
				h := skipList.head
				oldLevel := h.level
				if level <= oldLevel {
					break
				}
				newh := h
				oldbase := h.index.node
				for j := oldLevel + 1; j <= level; j++ {
					newh = &head{
						index: &index{
							node:  oldbase,
							down:  newh.index,
							right: idxs[j],
						},
						level: j,
					}
				}
				if skipList.casHead(h, newh) {
					h = newh
					level = oldLevel
					idx = idxs[level]
					break
				}
			}
		}
	splice:
		for insertionLevel := level; ; {
			j := h.level
			q := h.index
			r := q.right
			t := idx
			for {
				if q == nil || t == nil {
					return nil
				}
				if r != nil {
					n := r.node
					c := compareKeyAndSeq(entry.Key, entry.Seq, n.key, n.seq)
					if c > 0 {
						q = r
						r = r.right
						continue
					}
				}

				if j == insertionLevel {
					if !q.link(r, t) {
						break
					}
					insertionLevel--
					if insertionLevel == 0 {
						break splice
					}
				}

				j--
				if j >= insertionLevel && j < level {
					t = t.down
				}
				if q == nil || q.down == nil {
					return nil
				}
				q = q.down
				r = q.right
			}
		}
	}
	return nil
}

func (skipList *SkipList) doGet(key Key, seq uint64) *TableValue {
outer:
	for {
		b := skipList.findPredecessor(key, seq)
		n := b.next
		for {
			if n == nil {
				break outer
			}
			if bytes.Compare(key, n.key) == 0 && seq >= n.seq {
				return n.value
			}
			if bytes.Compare(key, b.key) == 0 && seq >= b.seq {
				return b.value
			}
			c := compareKeyAndSeq(key, seq, n.key, n.seq)
			if c < 0 {
				break outer
			}
			f := n.next
			b = n
			n = f
		}
	}
	return nil
}

func (skipList *SkipList) findPredecessor(key Key, seq uint64) *node {
	for {
		cur := skipList.head.index
		right := cur.right
		for {
			if right != nil {
				n := right.node
				c := compareKeyAndSeq(key, seq, n.key, n.seq)
				if c > 0 {
					cur = right
					right = right.right
					continue
				}
			}
			down := cur.down
			if down == nil {
				return cur.node
			}
			cur = down
			right = down.right
		}
	}
}
