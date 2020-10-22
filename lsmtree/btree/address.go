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
	"fmt"
	"sync/atomic"
	"yuyi-go/lsmtree/memtable"
	"yuyi-go/shared"

	"github.com/google/uuid"
)

const (
	FileNameLength = 16

	OffsetSize = 4
	LengthSize = 4
)

var fakeFile = [16]byte{}
var counter = int32(0)

// address the address to find from disk
type address struct {
	// File the name of the file.
	File [16]byte
	// Offset the start offset in the file of the address.
	Offset int
	// Length the length located in the file.
	Length int
}

// newFakeAddr fake address have empty file and negative offset, which is used
// to present address only in memory.
func newFakeAddr() address {
	atomic.AddInt32(&counter, 1)
	return address{
		File:   fakeFile,
		Offset: -int(counter),
	}
}

func (addr *address) String() string {
	return fmt.Sprintf("%s %d %d", uuid.UUID(addr.File).String(), addr.Offset, addr.Length)
}

func (addr *address) Value() memtable.Value {
	value := make([]byte, 0)
	buffer := bytes.NewBuffer(value)
	shared.WriteBytes(buffer, addr.File[0:])
	shared.WriteInt32(buffer, int32(addr.Offset))
	shared.WriteInt32(buffer, int32(addr.Length))
	return buffer.Bytes()
}

func (addr *address) equals(addr2 address) bool {
	return bytes.Compare(addr.File[0:], addr2.File[0:]) == 0 && addr.Offset == addr2.Offset && addr.Length == addr2.Length
}

func newAddress(input []byte) address {
	var file [16]byte
	copy(file[0:], input)

	buffer := bytes.NewBuffer(input[FileNameLength : FileNameLength+OffsetSize+LengthSize : FileNameLength+OffsetSize+LengthSize])
	return address{
		File:   file,
		Offset: int(shared.ReadInt32(buffer)),
		Length: int(shared.ReadInt32(buffer)),
	}
}
