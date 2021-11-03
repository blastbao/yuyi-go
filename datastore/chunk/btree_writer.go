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
	"github.com/blastbao/yuyi-go/shared"
	"io"
)

type BtreeWriter struct {
	// chunk the chunk file that is writing to
	chunk *chunk

	// offset the offset of position in the chunk file to write
	offset int

	// writer the inner writer to handle writing
	writer io.Writer

	// channel the channel to accept write request
	channel chan []byte

	// cfg the configuration instance
	cfg *shared.Config
}

func NewBtreeWriter(cfg *shared.Config) (*BtreeWriter, error) {
	c, err := newChunk(btree, cfg)
	if err != nil {
		return nil, err
	}
	writer := newChainedBtreeWriter(c)
	return &BtreeWriter{
		chunk:  c,
		offset: 0,
		writer: writer,
	}, nil
}

func (w *BtreeWriter) Write(p []byte) (addr Address, err error) {
	// try rotate chunk if current chunk is nil
	if w.chunk == nil {
		err = w.rotateBTree()
		if err != nil {
			return addr, err
		}
	}

	len := len(p)
	// check if chunk is full
	if w.offset+len >= w.chunk.capacity {
		err = w.rotateBTree()
		if err != nil {
			return addr, err
		}
	}
	written, err := w.writer.Write(p)
	if err != nil {
		err2 := w.rotateBTree()
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

func (w *BtreeWriter) rotateBTree() error {
	chunk, err := newChunk(btree, w.cfg)
	if err != nil {
		w.chunk = nil // set chunk nil as origin chunk is no longer available to write
		return err
	}
	// set new chunk and new offset for writer
	w.chunk = chunk
	w.offset = 0
	w.writer = newChainedBtreeWriter(chunk)
	return nil
}

// newChainedBtreeWriter add crc32 check checksum segment and snappy compression when writing disk
func newChainedBtreeWriter(c *chunk) io.Writer {
	return &crc32Writer{
		writer: &snappyWriter{
			writer: &fileWriter{
				file: chunkFileName(c.name, btree),
			},
		},
	}
}
