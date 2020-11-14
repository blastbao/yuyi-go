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
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type WalWriter struct {
	// chunk current chunk to write to
	chunk *chunk

	// offset current offset that is writing
	offset int

	// writer the inner writer to handle actual writing
	writer io.Writer

	// writeItemQueue the queue to hold write items
	writeItemQueue chan *writeItem

	// batchLimit the limit of size of each batch write request
	batchLimit int

	// lingerTime the max delay for processing write items
	lingerTime time.Duration
}

func NewWalWriter() (*WalWriter, error) {
	c, err := newChunk(wal)
	if err != nil {
		return nil, err
	}
	writer := &WalWriter{
		chunk:          c,
		offset:         0,
		writeItemQueue: make(chan *writeItem),
		batchLimit:     4 * 1024,
		lingerTime:     20 * time.Millisecond,
	}
	// start batch processor
	go writer.batchProcessor()
	return writer, nil
}

// callback the callback function after async write finished
type callback func(addr Address, err error)

func (w *WalWriter) AsyncWrite(p []byte, completed chan error, callback callback) {
	writeItem := newWriteItem(p, completed, callback)
	w.writeItemQueue <- writeItem
}

func (w *WalWriter) batchProcessor() {
	var batch []*writeItem
	var length int
	lingerTimer := time.NewTimer(0)
	if !lingerTimer.Stop() {
		<-lingerTimer.C
	}
	defer lingerTimer.Stop()

	for {
		select {
		case item := <-w.writeItemQueue:
			if length+len(item.block) >= w.batchLimit {
				if err := w.batchWrite(batch, length); err != nil {
					w.errHandler(err, batch)
				}
				if !lingerTimer.Stop() {
					<-lingerTimer.C
				}
				// reset length and batch
				length = 0
				batch = make([]*writeItem, 0)
			}
			// not reach batch limit, append to batch
			batch = append(batch, item)
			length += len(item.block)
			if len(batch) == 1 {
				lingerTimer.Reset(w.lingerTime)
			}
		case <-lingerTimer.C:
			if err := w.batchWrite(batch, length); err != nil {
				w.errHandler(err, batch)
			}
			// reset length and batch
			length = 0
			batch = make([]*writeItem, 0)
		}
	}
	fmt.Println("Batch processor loop quit")
}

func (w *WalWriter) batchWrite(items []*writeItem, length int) (err error) {
	// try rotate chunk if current chunk is nil
	if w.chunk == nil {
		err = w.rotateWal()
		if err != nil {
			w.errHandler(err, items)
			return err
		}
	}

	var f *os.File
	f, err = os.OpenFile(chunkFileName(w.chunk.name, wal), os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		w.errHandler(err, items)
		return err
	}
	defer f.Close()

	newWriter := bufio.NewWriterSize(f, length)
	for _, item := range items {
		if _, err = newWriter.Write(item.block); err != nil {
			w.errHandler(err, items)
			return err
		}
	}

	if err = newWriter.Flush(); err != nil {
		w.errHandler(err, items)
		return err
	}

	// callback after writer finished
	for _, item := range items {
		written := len(item.block)
		addr := Address{
			Chunk:  w.chunk.name,
			Offset: w.offset,
			Length: written,
		}
		w.offset += written
		if item.callback != nil {
			item.callback(addr, nil)
		}

		// notify caller that the write completed
		item.complete <- nil
	}
	return nil
}

func (w *WalWriter) errHandler(err error, items []*writeItem) {
	// callback for each write item to notify the writing error
	for _, item := range items {
		item.callback(Address{}, err)

		// notify caller that the write completed
		item.complete <- err
	}
}

func (w *WalWriter) rotateWal() error {
	chunk, err := newChunk(wal)
	if err != nil {
		w.chunk = nil // set chunk nil as origin chunk is no longer available to write
		return err
	}
	// set new chunk and new offset for writer
	w.chunk = chunk
	w.offset = 0
	return nil
}

type writeItem struct {
	// block the bytes to write
	block []byte

	// complete the channel to notify task finished
	complete chan error

	// callback the callback function after write finished
	callback callback
}

func newWriteItem(p []byte, completed chan error, callback callback) *writeItem {
	chechsum := crc32.ChecksumIEEE(p)
	// put length of block first
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(p)+4)) // 4 is length checksum
	p = append(lenBytes, p...)
	return &writeItem{
		block:    append(p, byte(chechsum>>24), byte(chechsum>>16), byte(chechsum>>8), byte(chechsum)),
		complete: completed,
		callback: callback,
	}
}
