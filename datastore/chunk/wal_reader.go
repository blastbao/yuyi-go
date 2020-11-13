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
	"hash/crc32"
	"io"
	"os"

	"github.com/google/uuid"
)

var (
	// batchSize the max read length from wal chunk one round
	batchSize = 64 * 1024
)

// WalReader read wal blocks from start seq to last seq and start from the offset.
type WalReader struct {
	ds        uuid.UUID
	seq       uint64
	lastSeq   uint64
	interrupt chan error
	reader    *bufio.Reader
}

func NewWalReader(ds uuid.UUID, startSeq uint64, offset int) (*WalReader, error) {
	reader, err := newChunkReader(startSeq, offset)
	if err != nil {
		return nil, err
	}
	interrupt := make(chan error, 1)
	return &WalReader{
		ds:        ds,
		seq:       startSeq,
		lastSeq:   walSeq,
		interrupt: interrupt,
		reader:    reader,
	}, nil
}

func (r *WalReader) Replay(complete chan error) (blockChan chan []byte) {
	// start a goroutine to replay wal async
	blockChan = make(chan []byte, 10) // Todo: make the chan bufferred length configuratable
	go r.replay(blockChan, complete)
	return blockChan
}

func newChunkReader(seq uint64, offset int) (*bufio.Reader, error) {
	name := newChunkNameWithSeq(seq)
	f, err := os.Open(chunkFileName(name, wal))
	if err != nil {
		return nil, err
	}
	f.Seek(int64(offset), 0)
	return bufio.NewReaderSize(f, batchSize), nil
}

func (r *WalReader) replay(blockChan chan []byte, complete chan error) {
	lenBytes := make([]byte, 4)
	for {
		// check for interrupt
		select {
		case <-r.interrupt:
			return
		default:
			// do nothing just prevernt interrupt checking block the replay
		}

		// check if all chunk replayed
		if r.seq > r.lastSeq {
			complete <- nil
			return
		}

		// read next block's length
		var err error
		_, err = io.ReadFull(r.reader, lenBytes)
		if err != nil {
			if err == io.EOF {
				//Todo: Should take chunk file sealed length into account
				r.seq++
				continue
			}
		}
		length := binary.BigEndian.Uint32(lenBytes)

		// read next block's content
		block := make([]byte, length)
		_, err = io.ReadFull(r.reader, block)
		if err != nil {
			//Todo: log panic
			// failed to read block from wal chunk
			complete <- err
		}
		block = block[4:length:length]

		// check next block's ds id
		if !r.owned(block) {
			continue
		}

		// validate next block's crc32 checksum
		blockLen := len(block)
		chechsum := crc32.ChecksumIEEE(block[0 : blockLen-4])
		if block[blockLen-4] != byte(chechsum>>24) ||
			block[blockLen-3] != byte(chechsum>>16) ||
			block[blockLen-2] != byte(chechsum>>8) ||
			block[blockLen-1] != byte(chechsum) {
			// crc32 checksum mismatch
			complete <- ErrUnexpectedCheckSum
			return
		}

		// enqueue block to blockChan without checksum
		blockChan <- block[0 : blockLen-4 : blockLen-4]
	}
}

func (r *WalReader) owned(block []byte) bool {
	for i := 0; i < 16; i++ {
		if r.ds[i] != block[i] {
			return false
		}
	}
	return true
}
