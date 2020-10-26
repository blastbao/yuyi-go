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
	"io"
	"sync"
)

type ChunkWrtier interface {
	Write(p []byte) (addr Address, err error)

	BatchWrite(p [][]byte) (addrs []Address, err error)
}

type btreeWriter struct {
	chunk  *chunk
	offset int
	mutex  sync.Mutex
	piped  io.Writer
}

func (writer *btreeWriter) Write(p []byte) (Address, error) {
	len := len(p)
	// check if chunk is full
	if writer.offset+len >= writer.chunk.capacity {
		err := writer.rotate()
		if err != nil {
			return Address{}, err
		}
	}
	_, err := writer.piped.Write(p)
	if err != nil {
		return Address{}, err
	}
	addr := Address{
		Chunk:  writer.chunk.name,
		Offset: writer.offset,
		Length: len,
	}
	writer.offset += len
	return addr, nil
}

func (writer *btreeWriter) rotate() error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	chunk, err := NewChunk()
	if err != nil {
		return err
	}
	// set new chunk and new offset for writer
	writer.chunk = chunk
	writer.offset = 0
	return nil
}
