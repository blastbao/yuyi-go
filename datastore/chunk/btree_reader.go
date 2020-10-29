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

type btreeReader struct {
	reader ChunkReader
}

func NewBtreeReader() (*btreeReader, error) {
	reader := newChainedBTreeReader()
	return &btreeReader{
		reader: reader,
	}, nil
}

func (r *btreeReader) Read(addr Address) (p []byte, err error) {
	return r.reader.Read(addr)
}

func newChainedBTreeReader() ChunkReader {
	return &crc32Reader{
		reader: &snappyReader{
			reader: &fileReader{
				chunkType: btree,
			},
		},
	}
}
