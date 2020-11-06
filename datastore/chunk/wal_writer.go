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
)

type walWriter struct {
	chunk       *chunk
	offset      int
	innerWriter io.Writer

	completed map[string]chan writeTask
}

func newWalWriter() (*walWriter, error) {
	c, err := newChunk(wal)
	if err != nil {
		return nil, err
	}
	chainedWriter := newChainedWalWriter(c)
	writer := &walWriter{
		chunk:       c,
		offset:      0,
		innerWriter: chainedWriter,
	}
	return writer, nil
}

func (w *walWriter) write(p []byte) (addr Address, err error) {
	// try rotate chunk if current chunk is nil
	if w.chunk == nil {
		err = w.rotateWal()
		if err != nil {
			return addr, err
		}
	}

	len := len(p)
	// check if chunk is full
	if w.offset+len >= w.chunk.capacity {
		err = w.rotateWal()
		if err != nil {
			return addr, err
		}
	}
	written, err := w.innerWriter.Write(p)
	if err != nil {
		err2 := w.rotateWal()
		if err != nil {
			return addr, err2
		}
		return addr, err
	}
	addr = Address{
		Chunk:  w.chunk.name,
		Offset: w.offset,
		Length: written,
	}
	w.offset += written
	return addr, nil
}

func (w *walWriter) CompletedTask(ds string) chan writeTask {
	return w.completed[ds]
}

func (w *walWriter) rotateWal() error {
	chunk, err := newChunk(wal)
	if err != nil {
		w.chunk = nil // set chunk nil as origin chunk is no longer available to write
		return err
	}
	// set new chunk and new offset for innerWriter
	w.chunk = chunk
	w.offset = 0
	w.innerWriter = newChainedWalWriter(chunk)
	return nil
}

// newChainedWalWriter add crc32 check checksum segment and snappy compression when writing disk
func newChainedWalWriter(c *chunk) io.Writer {
	return &crc32Writer{
		writer: &snappyWriter{
			writer: &fileWriter{
				file: chunkFileName(c.name, wal),
			},
		},
	}
}
