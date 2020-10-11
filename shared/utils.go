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

package shared

import (
	"bytes"
	"encoding/binary"
	"log"
)

func BytesCompare(bytes1 []byte, bytes2 []byte) int {
	len1 := len(bytes1)
	len2 := len(bytes2)

	for i := 0; i < len1 && i < len2; i++ {
		byte1 := int(bytes1[i])
		byte2 := int(bytes2[i])

		compare := byte1 - byte2
		if compare != 0 {
			return compare
		}
	}
	return len1 - len2
}

func BytesCompareWithLength(bytes1 []byte, off1 int, len1 int, bytes2 []byte, off2 int, len2 int) int {
	for i := 0; i < len1 && i < len2; i++ {
		byte1 := int(bytes1[i+off1])
		byte2 := int(bytes2[i+off2])

		compare := byte1 - byte2
		if compare != 0 {
			return compare
		}
	}
	return len1 - len2
}

func ReadInt16(bytes []byte, off int) int16 {
	return int16(bytes[off])<<8 + int16(bytes[off+1])
}

func ReadInt(bytes []byte, off int) int {
	return int(bytes[off])<<24 + int(bytes[off+1])<<16 + int(bytes[off+2])<<8 + int(bytes[off+3])
}

func WriteByte(buffer *bytes.Buffer, c byte) {
	buffer.WriteByte(c)
}

func WriteString(buffer *bytes.Buffer, s string) {
	buffer.WriteString(s)
}

func WriteInt16(buffer *bytes.Buffer, i int16) {
	err := binary.Write(buffer, binary.BigEndian, i)
	if err != nil {
		log.Panic("Write short to buffer failed.")
	}
}

func WriteInt32(buffer *bytes.Buffer, i int32) {
	err := binary.Write(buffer, binary.BigEndian, i)
	if err != nil {
		log.Panic("Write int to buffer failed.")
	}
}
