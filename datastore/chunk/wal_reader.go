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
	"os"

	"github.com/google/uuid"
)

var (
	// batchLen the max read length from wal chunk one round
	batchLen = 64 * 1024
)

type walReader struct {
	reader ChunkReader
}

func NewWalReader() (*walReader, error) {
	reader := newChainedWalReader()
	return &walReader{
		reader: reader,
	}, nil
}

func (r *walReader) Read(chunk uuid.UUID, offset int) (p []byte, err error) {
	f, err := os.Open(chunkFileName(chunk, wal))
	defer f.Close()
	if err != nil {
		return nil, err
	}
	f.Seek(int64(offset), 0)

	res := make([]byte, batchLen) // read max to batchLen one round

	reader := bufio.NewReader(f)
	n, err := reader.Read(res)
	if n != batchLen {
		res = res[0:n:n]
	}
	return res, nil
}

func newChainedWalReader() ChunkReader {
	return &crc32Reader{
		reader: &snappyReader{
			reader: &fileReader{
				chunkType: wal,
			},
		},
	}
}
