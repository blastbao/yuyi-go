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

package chunk

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"yuyi-go/shared"

	"github.com/google/uuid"
)

const (
	chunkLength = 16

	offsetSize = 4
	lengthSize = 4
)

var fakeFile = [16]byte{}
var counter = int32(0)

// Address the address to find from disk
type Address struct {
	// File the name of the file.
	Chunk uuid.UUID
	// Offset the start offset in the file of the address.
	Offset int
	// Length the length located in the file.
	Length int
}

// NewFakeAddress fake address have empty file and negative offset, which is used
// to present address only in memory.
func NewFakeAddress() Address {
	atomic.AddInt32(&counter, 1)
	return Address{
		Chunk:  fakeFile,
		Offset: -int(counter),
	}
}

func NewAddress(chunk uuid.UUID, offset int, length int) Address {
	return Address{
		Chunk:  chunk,
		Offset: offset,
		Length: length,
	}
}

func ParseAddress(input []byte) Address {
	var chunk [16]byte
	copy(chunk[0:], input)

	buffer := bytes.NewBuffer(input[chunkLength : chunkLength+offsetSize+lengthSize : chunkLength+offsetSize+lengthSize])
	return Address{
		Chunk:  chunk,
		Offset: int(shared.ReadInt32(buffer)),
		Length: int(shared.ReadInt32(buffer)),
	}
}

func (addr Address) String() string {
	return fmt.Sprintf("%s %d %d", uuid.UUID(addr.Chunk).String(), addr.Offset, addr.Length)
}

func (addr Address) Bytes() []byte {
	value := make([]byte, 0)
	buffer := bytes.NewBuffer(value)
	shared.WriteBytes(buffer, addr.Chunk[0:])
	shared.WriteInt32(buffer, int32(addr.Offset))
	shared.WriteInt32(buffer, int32(addr.Length))
	return buffer.Bytes()
}

func (addr *Address) Equals(addr2 Address) bool {
	return bytes.Compare(addr.Chunk[0:], addr2.Chunk[0:]) == 0 && addr.Offset == addr2.Offset && addr.Length == addr2.Length
}
