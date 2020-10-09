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

package btree

import (
	"bytes"
	"yuyi-go/lsmtree/memtable"
	"yuyi-go/shared"
)

type PageType int8

const (
	Root  PageType = 1
	Index PageType = 2
	Leaf  PageType = 3
	Blank PageType = 4
)

const (
	Version1 = byte(1)

	TypeOffset    = 1
	KVCountOffset = 2
	KeyOffset     = 4

	VersionSize     = 1
	TypeSize        = 1
	KeyOffsetSize   = 4
	ValueOffsetSize = 4
)

// PageBuffer the basic unit for a btree.
type PageBuffer struct {
	Content []byte
	Entries []*memtable.KVPair
	Address Address
}

// Type get the type of the page
func (page *PageBuffer) Type() PageType {
	switch page.Content[TypeOffset] {
	case 1:
		return Root
	case 2:
		return Index
	case 3:
		return Leaf
	default:
		return Blank
	}
}

// KVPairsCount
func (page *PageBuffer) KVPairsCount() int {
	if page.Entries != nil {
		return len(page.Entries)
	}
	return int(shared.ReadInt16(page.Content, KVCountOffset))
}

// Search find the location with the specified key.
func (page *PageBuffer) Search(key *memtable.Key) int {
	low := 0
	high := page.KVPairsCount() - 1

	for low <= high {
		mid := (low + high) / 2
		res := shared.BytesCompare(page.Key(mid), *key)

		if res < 0 {
			low = mid + 1
		} else if res > 0 {
			high = mid - 1
		} else {
			return mid
		}
	}
	return -(low + 1) // key not found.
}

// Key get key in the page with the specified index
func (page *PageBuffer) Key(index int) memtable.Key {
	if page.Entries != nil {
		return page.Entries[index].Key
	}
	var start int
	if index == 0 {
		start = KeyOffset
	} else {
		start = shared.ReadInt(page.Content, KeyOffset+int(index-1)*KeyOffsetSize)
	}
	end := shared.ReadInt(page.Content, KeyOffset+int(index)*KeyOffsetSize)
	return page.Content[start:end:end]
}

// Value get value in the page with the specified index
func (page *PageBuffer) Value(index int) memtable.Value {
	if page.Entries != nil {
		return page.Entries[index].Value
	}
	kvCount := page.KVPairsCount()
	valueOffset := KeyOffset + kvCount*KeyOffsetSize
	var start int
	if index == 0 {
		start = KeyOffset + kvCount*KeyOffsetSize
	} else {
		start = shared.ReadInt(page.Content, valueOffset+(index-1)*KeyOffsetSize)
	}
	end := shared.ReadInt(page.Content, valueOffset+index*KeyOffsetSize)
	return page.Content[start:end:end]
}

// Address get the child page address in the page with the specified index
func (page *PageBuffer) ChildAddress(index int) Address {
	value := page.Value(index)
	return CreateAddress(value)
}

// KVPair get key/value pair in the page with the specified index
func (page *PageBuffer) KVPair(index int) *memtable.KVPair {
	if page.Entries != nil {
		return page.Entries[index]
	}
	return &memtable.KVPair{Key: page.Key(index), Value: page.Value(index)}
}

// FloorEntry
func (page *PageBuffer) FloorEntry(key *memtable.Key) *memtable.KVPair {
	return nil
}

// CeilingEntry
func (page *PageBuffer) CeilingEntry(key *memtable.Key) *memtable.KVPair {
	return nil
}

// AllEntries get all key-value entries of the page buffer
func (page *PageBuffer) AllEntries() []*memtable.KVPair {
	if page.Entries == nil {
		kvCount := page.KVPairsCount()
		page.Entries = make([]*memtable.KVPair, kvCount)
		for i := int(0); i < kvCount; i++ {
			page.Entries[i] = &memtable.KVPair{Key: page.Key(i), Value: page.Value(i)}
		}
	}
	return page.Entries
}

type PageBufferForDump struct {
	// PageBuffer the page for dump
	PageBuffer

	// dirty if the page's content is modified
	dirty bool

	// valid if the page is valid to writing to disk
	valid bool

	// size the size of the page
	size int

	// shadowKey the deleted mapping key
	shadowKey memtable.Key
}

func (page *PageBufferForDump) AddKV(key memtable.Key, value memtable.Value, index int) {
	entries := page.AllEntries()
	if index < 0 {
		index = -index - 1
		// update size of the page
		page.size += len(key) + len(value)

		leftPart := entries[0:index:index]
		right := entries[index:len(entries):len(entries)]
		leftPart = append(leftPart, &memtable.KVPair{Key: key, Value: value})
		entries = append(leftPart, right...)
	} else {
		// update size of the page
		page.size -= len(entries[index].Key) + len(entries[index].Value)
		page.size += len(key) + len(value)

		entries[index] = &memtable.KVPair{Key: key, Value: value}
	}
	// update entries of the page
	page.Entries = entries
}

func (page *PageBufferForDump) mappingKey() memtable.Key {
	if page.shadowKey != nil {
		return page.shadowKey
	}
	return page.Key(0)
}

func (page *PageBufferForDump) addKVPair(pair *memtable.KVPair) {
	index := page.Search(&pair.Key)
	page.AddKV(pair.Key, pair.Value, index)
}

func (page *PageBufferForDump) addKVEntryToIndex(entry *memtable.KVEntry, index int) {
	page.AddKV(entry.Key, entry.TableValue.Value, index)
}

func (page *PageBufferForDump) removeKVEntry(index int) {
	entries := page.AllEntries()

	leftPart := entries[0:index:index]
	right := entries[index+1 : len(entries) : len(entries)]
	page.Entries = append(leftPart, right...)
}

func (page *PageBufferForDump) buildCompressedBytes() []byte {
	if !page.dirty {
		return page.Content
	}
	// calculate capacity of the content
	kvCount := page.KVPairsCount()

	keyOffset := VersionSize + TypeSize*kvCount*(KeyOffsetSize+ValueOffsetSize)
	capacity := keyOffset
	valueOffset := keyOffset
	for _, entry := range page.AllEntries() {
		capacity += len(entry.Key) + len(entry.Value)
		valueOffset += len(entry.Key)
	}
	res := make([]byte, capacity)
	buffer := bytes.NewBuffer(res)

	shared.WriteByte(buffer, Version1)
	shared.WriteByte(buffer, byte(page.Type()))
	// write key offset
	for _, entry := range page.AllEntries() {
		shared.WriteInt(buffer, keyOffset+len(entry.Key))
		keyOffset += len(entry.Key)
	}
	// write value offset
	for _, entry := range page.AllEntries() {
		shared.WriteInt(buffer, valueOffset+len(entry.Value))
		valueOffset += len(entry.Value)
	}
	// write key content
	for _, entry := range page.AllEntries() {
		buffer.Write(entry.Key)
	}
	// write value content
	for _, entry := range page.AllEntries() {
		buffer.Write(entry.Value)
	}
	return res
}
