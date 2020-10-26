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
	"yuyi-go/datastore/chunk"

	"github.com/google/uuid"
)

// MaxLength set max capacity of each file to 512k
const MaxLength = 512 * 1024

var (
	file = uuid.New()
	off  = 0

	cache = map[chunk.Address][]byte{}
)

func writeTo(input []byte) chunk.Address {
	size := len(input)
	if off+size > MaxLength {
		// need rotate to another file
		file = uuid.New()
		off = 0
	}
	// create new address and cache it.
	res := chunk.Address{Chunk: file, Offset: off, Length: size}
	cache[res] = input
	off += size
	return res
}

func ReadFrom(addr chunk.Address) []byte {
	return cache[addr]
}
