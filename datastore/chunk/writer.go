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
	"hash/crc32"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/google/uuid"
)

type ChunkWrtier interface {
	Write(p []byte) (addr Address, err error)
}

type btreeWriter struct {
	chunk  *chunk
	offset int
	writer io.Writer
}

func newBtreeWriter() (*btreeWriter, error) {
	c, err := newChunk()
	if err != nil {
		return nil, err
	}
	writer := newChainedWriter(c)
	return &btreeWriter{
		chunk:  c,
		offset: 0,
		writer: writer,
	}, nil
}

func (w *btreeWriter) Write(p []byte) (addr Address, err error) {
	// try rotate chunk if current chunk is nil
	if w.chunk == nil {
		err = w.rotate()
		if err != nil {
			return addr, err
		}
	}

	len := len(p)
	// check if chunk is full
	if w.offset+len >= w.chunk.capacity {
		err = w.rotate()
		if err != nil {
			return addr, err
		}
	}
	_, err = w.writer.Write(p)
	if err != nil {
		err2 := w.rotate()
		if err != nil {
			return addr, err2
		}
		return addr, err
	}
	addr = Address{
		Chunk:  w.chunk.name,
		Offset: w.offset,
		Length: len,
	}
	w.offset += len
	return addr, nil
}

func (w *btreeWriter) rotate() error {
	chunk, err := newChunk()
	if err != nil {
		w.chunk = nil // set chunk nil as origin chunk is no longer available to write
		return err
	}
	// set new chunk and new offset for writer
	w.chunk = chunk
	w.offset = 0
	w.writer = newChainedWriter(chunk)
	return nil
}

// newChainedWriter add crc32 check checksum segment and snappy compression when writing disk
func newChainedWriter(c *chunk) io.Writer {
	return &crc32Writer{
		writer: &snappyWriter{
			writer: &fileWriter{
				file: chunkFileName(c.name),
			},
		},
	}
}

type crc32Writer struct {
	chunk  uuid.UUID
	writer io.Writer
}

func (w *crc32Writer) Write(p []byte) (n int, err error) {
	chechsum := crc32.ChecksumIEEE(p)

	block := make([]byte, len(p)+16+4) // 16 length of uuid, 4 length of checksum
	block = append(block, w.chunk[0:]...)
	block = append(block, p...)
	block = append(block, byte(chechsum>>24), byte(chechsum>>16), byte(chechsum>>8), byte(chechsum))
	return w.writer.Write(block)
}

type snappyWriter struct {
	writer io.Writer
}

func (w *snappyWriter) Write(p []byte) (n int, err error) {
	block := snappy.Encode(nil, p)
	return w.writer.Write(block)
}

type fileWriter struct {
	file string
}

func (w *fileWriter) Write(p []byte) (n int, err error) {
	err = ioutil.WriteFile(w.file, p, 0666)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
