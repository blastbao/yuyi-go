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
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/google/uuid"
)

type crc32Reader struct {
	chunk  uuid.UUID
	writer io.Reader
}

func (r *crc32Reader) Read(p []byte) (n int, err error) {
	chechsum := crc32.ChecksumIEEE(p)

	block := make([]byte, len(p)+16+4) // 16 length of uuid, 4 length of checksum
	block = append(block, r.chunk[0:]...)
	block = append(block, p...)
	block = append(block, byte(chechsum>>24), byte(chechsum>>16), byte(chechsum>>8), byte(chechsum))
	return r.writer.Read(block)
}

type snappyReader struct {
	r io.Reader
}

func (r *snappyReader) Read(p []byte) (n int, err error) {
	_, err = r.Read(p)
	if err != nil {
		return 0, err
	}
	_, err = snappy.Decode(p, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

type fileReader struct {
	addr Address
}

func (r *fileReader) Read(p []byte) (n int, err error) {
	addr := r.addr
	file, err := os.Open(chunkFileName(addr.Chunk))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	_, err = file.ReadAt(p, int64(addr.Offset))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
