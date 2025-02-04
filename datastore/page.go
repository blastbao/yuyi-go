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
	"github.com/blastbao/yuyi-go/shared"
)

type PageType byte

const (
	Root  PageType = 1
	Index PageType = 2
	Leaf  PageType = 3
	Blank PageType = 4
)

const (
	Version1 = byte(1)

	VersionSize     = 1
	TypeSize        = 1
	KVCountSize     = 2
	KVOffsetSize    = 4
	ValueOffsetSize = 4

	TypeOffset    = VersionSize
	KVCountOffset = VersionSize + TypeSize
	KeyOffset     = VersionSize + TypeSize + KVCountSize
)

// page the basic unit for a btree.
type page struct {
	content []byte
	entries []*KVPair
	addr    chunk.Address
}

func NewPage(pageType PageType, pairs []*KVPair) *page {
	return &page{
		content: newEmptyContent(pageType),
		entries: pairs,
		addr:    chunk.NewFakeAddress(),
	}
}

func newEmptyContent(pageType PageType) []byte {
	var array [VersionSize + TypeSize + KVCountSize]byte
	array[0] = Version1
	array[1] = byte(pageType)
	return array[0:]
}

// Type get the type of the page
func (page *page) Type() PageType {
	switch page.content[TypeOffset] {
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
func (page *page) KVPairsCount() int {
	if page.entries != nil {
		return len(page.entries)
	}
	return int(shared.ReadInt16(
		bytes.NewBuffer(page.content[KVCountOffset : KVCountOffset+KVCountSize : KVCountOffset+KVCountSize])))
}

// Search find the location with the specified key.
func (page *page) Search(key *Key) int {
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
func (page *page) Key(index int) Key {
	if page.entries != nil {
		return page.entries[index].Key
	}
	var start int
	if index == 0 {
		start = KeyOffset + page.KVPairsCount()*KVOffsetSize*2
	} else {
		offset := KeyOffset + (index-1)*KVOffsetSize
		start = int(shared.ReadInt32(bytes.NewBuffer(page.content[offset : offset+KVOffsetSize : offset+KVOffsetSize])))
	}
	offset := KeyOffset + index*KVOffsetSize
	end := int(shared.ReadInt32(bytes.NewBuffer(page.content[offset : offset+KVOffsetSize : offset+KVOffsetSize])))
	return page.content[start:end:end]
}

// Value get value in the page with the specified index
func (page *page) Value(index int) Value {
	if page.entries != nil {
		return page.entries[index].Value
	}
	kvCount := page.KVPairsCount()
	valueOffset := KeyOffset + kvCount*KVOffsetSize
	var start int
	if index == 0 {
		offset := KeyOffset + (kvCount-1)*KVOffsetSize
		start = int(shared.ReadInt32(bytes.NewBuffer(page.content[offset : offset+KVOffsetSize : offset+KVOffsetSize])))
	} else {
		offset := valueOffset + (index-1)*KVOffsetSize
		start = int(shared.ReadInt32(bytes.NewBuffer(page.content[offset : offset+KVOffsetSize : offset+KVOffsetSize])))
	}
	offset := valueOffset + index*KVOffsetSize
	end := int(shared.ReadInt32(bytes.NewBuffer(page.content[offset : offset+KVOffsetSize : offset+KVOffsetSize])))
	return page.content[start:end:end]
}

// address get the child page address in the page with the specified index
func (page *page) ChildAddress(index int) chunk.Address {
	value := page.Value(index)
	return chunk.ParseAddress(value)
}

// KVPair get key/value pair in the page with the specified index
func (page *page) KVPair(index int) *KVPair {
	if page.entries != nil {
		return page.entries[index]
	}
	return &KVPair{Key: page.Key(index), Value: page.Value(index)}
}

// FloorEntry
func (page *page) FloorEntry(key *Key) *KVPair {
	return nil
}

// CeilingEntry
func (page *page) CeilingEntry(key *Key) *KVPair {
	return nil
}

// AllEntries get all key-value entries of the page buffer
func (page *page) AllEntries() []*KVPair {
	if page.entries == nil {
		kvCount := page.KVPairsCount()
		slice := make([]*KVPair, kvCount)
		for i := 0; i < kvCount; i++ {
			slice[i] = &KVPair{Key: page.Key(i), Value: page.Value(i)}
		}
		page.entries = slice
	}
	return page.entries
}

type pageForDump struct {
	// page the page for dump
	page

	// dirty if the page's content is modified
	dirty bool

	// valid if the page is valid to writing to disk
	valid bool

	// size the size of the page
	size int

	// shadowKey the deleted mapping key
	shadowKey Key
}

func NewPageForDump(pageType PageType, pairs []*KVPair) *pageForDump {
	page := page{
		content: newEmptyContent(pageType),
		entries: pairs,
		addr:    chunk.NewFakeAddress(),
	}
	return &pageForDump{
		page:      page,
		dirty:     true,
		valid:     true,
		size:      len(page.content),
		shadowKey: nil,
	}
}

func (page *pageForDump) appendKVEntries(entriesToAppend []*KVPair) {
	// update entries of the page
	for _, pair := range entriesToAppend {
		page.entries = append(page.AllEntries(), pair)
		page.size += len(pair.Key) + len(pair.Value)
	}
	page.dirty = true
}

func (page *pageForDump) addKVToIndex(key Key, value Value, index int) {
	entries := page.AllEntries()
	if index < 0 {
		index = -index - 1
		// update size of the page
		page.size += len(key) + len(value)
		if index == 0 && page.shadowKey == nil && len(entries) != 0 {
			page.shadowKey = entries[0].Key
		}
		leftPart := entries[0:index:index]
		right := entries[index:len(entries):len(entries)]
		leftPart = append(leftPart, &KVPair{Key: key, Value: value})
		entries = append(leftPart, right...)
	} else {
		// update size of the page
		page.size -= len(entries[index].Key) + len(entries[index].Value)
		page.size += len(key) + len(value)

		entries[index] = &KVPair{Key: key, Value: value}
	}
	// update entries of the page
	page.entries = entries
	page.dirty = true
}

func (page *pageForDump) mappingKey() Key {
	if page.shadowKey != nil {
		return page.shadowKey
	}
	return page.Key(0)
}

func (page *pageForDump) addKV(key Key, value Value) {
	index := page.Search(&key)
	page.addKVToIndex(key, value, index)
}

func (page *pageForDump) addKVPair(pair *KVPair) {
	index := page.Search(&pair.Key)
	page.addKVToIndex(pair.Key, pair.Value, index)
}

func (page *pageForDump) addKVEntryToIndex(entry *KVEntry, index int) {
	page.addKVToIndex(entry.Key, entry.TableValue.Value, index)
}

func (page *pageForDump) removeKV(key Key) {
	index := page.Search(&key)
	if index >= 0 {
		page.removeKVEntryFromIndex(index)
	}
}

func (page *pageForDump) removeKVEntryFromIndex(index int) {
	entries := page.AllEntries()
	if index == 0 && page.shadowKey == nil {
		page.shadowKey = entries[0].Key
	}
	// update size of the page
	page.size -= len(entries[index].Key) + len(entries[index].Value)

	leftPart := entries[0:index:index]
	right := entries[index+1 : len(entries) : len(entries)]
	page.entries = append(leftPart, right...)
	page.dirty = true
}

func (page *pageForDump) buildBytes() []byte {
	if !page.dirty {
		return page.content
	}
	// calculate capacity of the content
	kvCount := page.KVPairsCount()

	keyOffset := VersionSize + TypeSize + KVCountSize + kvCount*(KVOffsetSize+ValueOffsetSize)
	capacity := keyOffset
	valueOffset := keyOffset
	for _, entry := range page.AllEntries() {
		capacity += len(entry.Key) + len(entry.Value)
		valueOffset += len(entry.Key)
	}
	res := make([]byte, 0, capacity)
	buffer := bytes.NewBuffer(res)

	shared.WriteByte(buffer, Version1)
	shared.WriteByte(buffer, byte(page.Type()))
	// write kv count
	shared.WriteInt16(buffer, int16(kvCount))
	// write key offset
	for _, entry := range page.AllEntries() {
		shared.WriteInt32(buffer, int32(keyOffset+len(entry.Key)))
		keyOffset += len(entry.Key)
	}
	// write value offset
	for _, entry := range page.AllEntries() {
		shared.WriteInt32(buffer, int32(valueOffset+len(entry.Value)))
		valueOffset += len(entry.Value)
	}
	// write key content
	for _, entry := range page.AllEntries() {
		shared.WriteBytes(buffer, entry.Key)
	}
	// write value content
	for _, entry := range page.AllEntries() {
		shared.WriteBytes(buffer, entry.Value)
	}
	return buffer.Bytes()
}
