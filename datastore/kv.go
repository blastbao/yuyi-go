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

import "bytes"

type OPERATION uint8

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

func (key Key) Compare(another Key) int {
	return bytes.Compare(key, another)
}
