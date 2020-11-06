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

	"github.com/google/uuid"
)

type OPERATION byte

const (
	Put       OPERATION = 1
	PutAbsent OPERATION = 2
	Remove    OPERATION = 3
	PageCopy  OPERATION = 4
)

type Key []byte

type Value []byte

type TableValue struct {
	Operation OPERATION
	Value     Value
}

type KVPair struct {
	Key   Key
	Value Value
}

type KVEntry struct {
	Key        Key
	TableValue TableValue
	Seq        uint64
}

func newKVEntry(key Key, value Value, oper OPERATION) *KVEntry {
	return &KVEntry{
		Key: key,
		TableValue: TableValue{
			Operation: oper,
			Value:     value,
		},
	}
}

// buildBytes create bytes of the KVEntry. The bytes will be written in wal chunk
func (entry *KVEntry) buildBytes(ds uuid.UUID) []byte {
	keyLen := len(entry.Key)
	resLen := 16 + 4 + keyLen // 16 is the length of the datastore id, 4 is the length of the key

	oper := entry.TableValue.Operation
	resLen++ // 1 is the length of operation
	if oper == Put || oper == PutAbsent {
		resLen += 4 + len(entry.TableValue.Value) // 4 is length of the value
	}

	res := make([]byte, 0, resLen)
	// datastore id part
	res = append(res, ds[0:]...)

	// key part
	res = append(res, byte(keyLen>>24), byte(keyLen>>16), byte(keyLen>>8), byte(keyLen))
	res = append(res, entry.Key...)

	// table value part
	res = append(res, byte(oper))
	if oper == Put || oper == PutAbsent {
		valueLen := len(entry.TableValue.Value)
		res = append(res, byte(valueLen>>24), byte(valueLen>>16), byte(valueLen>>8), byte(valueLen))
		res = append(res, entry.TableValue.Value...)
	}
	return res
}

func (key Key) Compare(another Key) int {
	return bytes.Compare(key, another)
}

func compareKVEntry(entry1 *KVEntry, entry2 *KVEntry) int {
	return compareKeyAndSeq(entry1.Key, entry1.Seq, entry2.Key, entry2.Seq)
}

func compareKeyAndSeq(key1 Key, seq1 uint64, key2 Key, seq2 uint64) int {
	res := key1.Compare(key2)
	if res != 0 {
		return res
	}
	return int(seq1 - seq2)
}
