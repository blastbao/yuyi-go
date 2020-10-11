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
)

const (
	FileNameLength = 36

	OffsetSize = 4
	LengthSize = 4
)

var fakeFile = "                                    "
var counter = int32(0)

// address the address to find from disk
type address struct {
	// File the name of the file.
	File string
	// Offset the start offset in the file of the address.
	Offset int
	// Length the length located in the file.
	Length int
}

// createFakeAddr fake address have empty file and negative offset, which is used
// to present address only in memory.
func createFakeAddr() address {
	atomic.AddInt32(&counter, 1)
	return address{
		File:   fakeFile,
		Offset: -int(counter),
	}
}

func (addr *address) ToString() string {
	return fmt.Sprintf("%s %d %d", addr.File, addr.Offset, addr.Length)
}

func (addr *address) ToValue() memtable.Value {
	value := make([]byte, 0)
	buffer := bytes.NewBuffer(value)
	shared.WriteString(buffer, addr.File)
	shared.WriteInt32(buffer, int32(addr.Offset))
	shared.WriteInt32(buffer, int32(addr.Length))
	return buffer.Bytes()
}

func CreateAddress(input []byte) address {
	buffer := bytes.NewBuffer(input[0 : FileNameLength+OffsetSize+LengthSize : FileNameLength+OffsetSize+LengthSize])
	return address{
		File:   buffer.String(),
		Offset: shared.ReadInt(input, FileNameLength),
		Length: shared.ReadInt(input, FileNameLength+OffsetSize),
	}
}
