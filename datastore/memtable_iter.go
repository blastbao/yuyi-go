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

import "fmt"

type iter interface {
	hasNext() bool
	next() *KVEntry
}

type listIter struct {
	skipList *SkipList
	current  *node
	endKey   Key
	seq      uint64
}

func newListIter(list *SkipList, start Key, end Key, seq uint64) *listIter {
	pre := list.findPredecessor(start, seq)
	return &listIter{
		skipList: list,
		current:  pre,
		endKey:   end,
		seq:      seq,
	}
}

func (iter *listIter) hasNext() bool {
	if iter.current == nil || iter.current.next == nil {
		return false
	}
	if iter.endKey != nil {
		n := iter.current.next
		return compareKeyAndSeq(n.key, n.seq, iter.endKey, iter.seq) < 0
	}
	return true
}

func (iter *listIter) next() *KVEntry {
	var next *node
	for {
		next = iter.current.next
		if next.next != nil && next.key.Compare(next.next.key) == 0 {
			if next.next.seq < iter.seq {
				// next's next have same key but larger sequence, move on to next's next
				iter.current = next
				continue
			}
		}
		break
	}
	iter.current = next
	return &KVEntry{
		Key:        next.key,
		TableValue: *next.value,
		Seq:        next.seq,
	}
}

type combinedIterItem struct {
	cur  *KVEntry
	iter *listIter
}

type combinedIter struct {
	minHeap *minHeap
}

func newCombinedIter(iters []*listIter) *combinedIter {
	minHeap := newMinHeap(len(iters))
	for _, iter := range iters {
		if iter.hasNext() {
			minHeap.insert(&combinedIterItem{
				cur:  iter.next(),
				iter: iter,
			})
		}
	}
	return &combinedIter{
		minHeap: minHeap,
	}
}

func (iter *combinedIter) hasNext() bool {
	return iter.minHeap.peek() != nil
}

func (iter *combinedIter) next() *KVEntry {
	entry := iter.nextEntry()
	for {
		top := iter.minHeap.peek()
		// check if top entry have same key, need move on to next
		if top == nil || entry.Key.Compare(top.cur.Key) != 0 {
			break
		}
		entry = iter.nextEntry()
	}
	return entry
}

func (iter *combinedIter) nextEntry() *KVEntry {
	curItem := iter.minHeap.remove()
	entry := curItem.cur
	// if curItem has next, move to next and insert item back
	if curItem.iter.hasNext() {
		curItem.cur = curItem.iter.next()
		iter.minHeap.insert(curItem)
	}
	return entry
}

type minHeap struct {
	content []*combinedIterItem
	size    int
	maxsize int
}

func newMinHeap(maxsize int) *minHeap {
	minHeap := &minHeap{
		content: make([]*combinedIterItem, 0),
		size:    0,
		maxsize: maxsize,
	}
	return minHeap
}

func (m *minHeap) leaf(index int) bool {
	if index >= (m.size/2) && index <= m.size {
		return true
	}
	return false
}

func (m *minHeap) parent(index int) int {
	return (index - 1) / 2
}

func (m *minHeap) leftchild(index int) int {
	return 2*index + 1
}

func (m *minHeap) rightchild(index int) int {
	return 2*index + 2
}

func (m *minHeap) insert(item *combinedIterItem) error {
	if m.size >= m.maxsize {
		return fmt.Errorf("Heal is ful")
	}
	m.content = append(m.content, item)
	m.size++
	m.upHeapify(m.size - 1)
	return nil
}

func (m *minHeap) swap(first, second int) {
	temp := m.content[first]
	m.content[first] = m.content[second]
	m.content[second] = temp
}

func (m *minHeap) upHeapify(index int) {
	for m.compareContent(index, m.parent(index)) < 0 {
		m.swap(index, m.parent(index))
		index = m.parent(index)
	}
}

func (m *minHeap) downHeapify(current int) {
	if m.leaf(current) {
		return
	}
	smallest := current
	leftChildIndex := m.leftchild(current)
	rightChildIndex := m.rightchild(current)
	// if current is smallest then return
	if leftChildIndex < m.size && m.compareContent(leftChildIndex, smallest) < 0 {
		smallest = leftChildIndex
	}
	if rightChildIndex < m.size && m.compareContent(rightChildIndex, smallest) < 0 {
		smallest = rightChildIndex
	}
	if smallest != current {
		m.swap(current, smallest)
		m.downHeapify(smallest)
	}
	return
}

func (m *minHeap) compareContent(left, right int) int {
	if m.content[left] == nil && m.content[right] == nil {
		return 0
	}
	if m.content[left] == nil {
		return 1
	}
	if m.content[right] == nil {
		return -1
	}
	return compareKVEntry(m.content[left].cur, m.content[right].cur)
}

func (m *minHeap) remove() *combinedIterItem {
	top := m.content[0]
	m.content[0] = m.content[m.size-1]
	m.content = m.content[:(m.size)-1]
	m.size--
	m.downHeapify(0)
	return top
}

func (m *minHeap) peek() *combinedIterItem {
	if len(m.content) == 0 {
		return nil
	}
	return m.content[0]
}
