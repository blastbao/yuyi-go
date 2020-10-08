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
	"yuyi-go/lsmtree/memtable"
	"yuyi-go/shared"
)

const (
	FileNameLength = 36

	OffsetSize = 4
	LengthSize = 4
)

// Address the address to find from disk
type Address struct {
	// File the name of the file.
	File string
	// Offset the start offset in the file of the address.
	Offset int
	// Length the length located in the file.
	Length int
}

func (address *Address) ToString() string {
	return fmt.Sprintf("%s %d %d", address.File, address.Offset, address.Length)
}

func (address *Address) ToValue() memtable.Value {
	value := make([]byte, FileNameLength+OffsetSize+LengthSize)
	buffer := bytes.NewBuffer(value)
	buffer.WriteString(address.File)
	shared.WriteInt(buffer, address.Offset)
	shared.WriteInt(buffer, address.Length)
	return value
}

func CreateAddress(input []byte) Address {
	buffer := bytes.NewBuffer(input[0:FileNameLength:FileNameLength])
	return Address{
		File:   buffer.String(),
		Offset: shared.ReadInt(input, FileNameLength),
		Length: shared.ReadInt(input, FileNameLength+OffsetSize),
	}
}
