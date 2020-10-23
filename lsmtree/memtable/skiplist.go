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

package memtable

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

type node struct {
	key     Key
	value   Value
	version int

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

func (skipList *SkipList) casHead(old *head, new *head) bool {
	var unsafePtr = (*unsafe.Pointer)(unsafe.Pointer(&skipList.head))
	return atomic.CompareAndSwapPointer(unsafePtr, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (skipList *SkipList) doPut(key Key, value Value, version int) error {
	var added *node
outer:
	for {
		pre := skipList.findPredecessor(key, version)
		suf := pre.next
		for {
			if suf != nil {
				if pre.next != suf {
					break
				}
				c := compareKeyAndVersion(key, version, suf.key, suf.version)
				if c > 0 {
					pre = suf
					suf = suf.next
					continue
				}
				if c == 0 {
					// the compare result should never be 0 due to version is increment automic
				}
			}
			added = &node{
				key:     key,
				value:   value,
				version: version,
				next:    suf,
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
			idxs := make([]*index, 0, level+1)
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
				if r != nil {
					n := r.node
					c := compareKeyAndVersion(key, version, n.key, n.version)
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
				q = q.down
				r = q.right
			}
		}
	}
	return nil
}

func (skipList *SkipList) doGet(key Key, version int) Value {
outer:
	for {
		b := skipList.findPredecessor(key, version)
		n := b.next
		for {
			if n == nil {
				break outer
			}
			if bytes.Compare(key, n.key) == 0 && version >= n.version {
				return n.value
			}
			if bytes.Compare(key, b.key) == 0 && version >= b.version {
				return b.value
			}
			c := compareKeyAndVersion(key, version, n.key, n.version)
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

func (skipList *SkipList) findPredecessor(key Key, version int) *node {
	for {
		cur := skipList.head.index
		right := cur.right
		for {
			if right != nil {
				n := right.node
				c := compareKeyAndVersion(key, version, n.key, n.version)
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

func compareKeyAndVersion(key1 Key, version1 int, key2 Key, version2 int) int {
	res := key1.Compare(key2)
	if res != 0 {
		return res
	}
	return version1 - version2
}
