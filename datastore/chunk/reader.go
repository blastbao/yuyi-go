// Copyright 2015 The yuyi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable lar or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"hash/crc32"
	"os"

	"github.com/golang/snappy"
)

type ChunkReader interface {
	Read(addr Address) (p []byte, err error)
}

type btreeReader struct {
	reader ChunkReader
}

func NewBtreeReader() (*btreeReader, error) {
	reader := newChainedReader()
	return &btreeReader{
		reader: reader,
	}, nil
}

func (r *btreeReader) Read(addr Address) (p []byte, err error) {
	return r.reader.Read(addr)
}

func newChainedReader() ChunkReader {
	return &crc32Reader{
		reader: &snappyReader{
			reader: &fileReader{},
		},
	}
}

type crc32Reader struct {
	reader ChunkReader
}

func (r *crc32Reader) Read(addr Address) (p []byte, err error) {
	p, err = r.reader.Read(addr)
	if err != nil {
		return nil, err
	}
	// validate crc32 checksum
	len := len(p)
	chechsum := crc32.ChecksumIEEE(p[0 : len-4])
	if p[len-4] != byte(chechsum>>24) || p[len-3] != byte(chechsum>>16) ||
		p[len-2] != byte(chechsum>>8) || p[len-1] != byte(chechsum) {
		return nil, CheckSumError{msg: "Invalid checksum"}
	}

	return p[0 : len-4 : len-4], nil
}

type snappyReader struct {
	reader ChunkReader
}

func (r *snappyReader) Read(addr Address) (p []byte, err error) {
	var block []byte
	block, err = r.reader.Read(addr)
	if err != nil {
		return nil, err
	}
	p, err = snappy.Decode(nil, block)
	if err != nil {
		return nil, err
	}
	return p, nil
}

type fileReader struct{}

func (r *fileReader) Read(addr Address) (p []byte, err error) {
	file, err := os.Open(chunkFileName(addr.Chunk))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	p = make([]byte, addr.Length)
	_, err = file.ReadAt(p, int64(addr.Offset))
	if err != nil {
		return nil, err
	}
	return p, nil
}

type CheckSumError struct {
	msg string
}

func (err CheckSumError) Error() string {
	return err.msg
}
