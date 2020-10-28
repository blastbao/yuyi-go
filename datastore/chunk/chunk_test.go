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
	"math/rand"
	"os"
	"testing"
)

func TestReaderAndWriter(t *testing.T) {
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.Mkdir(folder, os.ModePerm)
	}

	writer, err := newBtreeWriter()
	if err != nil {
		t.Error("Failed to create writer")
		return
	}

	// write to chunk
	count := 200
	inputs := make([][]byte, count)
	addrs := make([]Address, count)
	for i := 0; i < count; i++ {
		input := randomBytes((i+1)*100, defaultLetters)
		addr, err := writer.Write(input)
		if err != nil {
			t.Error("Failed to create writer")
			return
		}
		inputs[i] = input
		addrs[i] = addr
	}

	// read from chunk
	reader, err := NewBtreeReader()
	if err != nil {
		t.Error("Failed to create reader")
		return
	}
	for i, addr := range addrs {
		block, err := reader.Read(addr)
		if err != nil {
			t.Error("Failed to read address ", addr)
			return
		}
		if bytes.Compare(inputs[i], block) != 0 {
			t.Error("block read from addr mismatch ", addr)
			return
		}
	}
}

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
func randomBytes(n int, allowedChars ...[]rune) []byte {
	var letters []rune

	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return []byte(string(b))
}
