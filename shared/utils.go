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

func ReadString(buffer *bytes.Buffer, len int) string {
	res := make([]byte, len)
	err := binary.Read(buffer, binary.BigEndian, res)
	if err != nil {
		log.Panic("Write short to buffer failed.")
	}
	return string(res)
}

func ReadBytes(buffer *bytes.Buffer, len int) []byte {
	res := make([]byte, len)
	err := binary.Read(buffer, binary.BigEndian, res)
	if err != nil {
		log.Panic("Write bytes to buffer failed.")
	}
	return res
}

func ReadInt16(buffer *bytes.Buffer) int16 {
	var res int16
	err := binary.Read(buffer, binary.BigEndian, &res)
	if err != nil {
		log.Panic("Write short to buffer failed.")
	}
	return res
}

func ReadInt32(buffer *bytes.Buffer) int32 {
	var res int32
	err := binary.Read(buffer, binary.BigEndian, &res)
	if err != nil {
		log.Panic("Write short to buffer failed.")
	}
	return res
}

func WriteByte(buffer *bytes.Buffer, b byte) {
	err := binary.Write(buffer, binary.BigEndian, &b)
	if err != nil {
		log.Panic("Write byte to buffer failed.")
	}
}

func WriteBytes(buffer *bytes.Buffer, bytes []byte) {
	err := binary.Write(buffer, binary.BigEndian, &bytes)
	if err != nil {
		log.Panic("Write bytes to buffer failed.")
	}
}

func WriteString(buffer *bytes.Buffer, s string) {
	buffer.WriteString(s)
}

func WriteInt16(buffer *bytes.Buffer, i int16) {
	err := binary.Write(buffer, binary.BigEndian, &i)
	if err != nil {
		log.Panic("Write short to buffer failed.")
	}
}

func WriteInt32(buffer *bytes.Buffer, i int32) {
	err := binary.Write(buffer, binary.BigEndian, &i)
	if err != nil {
		log.Panic("Write int to buffer failed.")
	}
}
