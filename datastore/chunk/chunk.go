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
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
	"yuyi-go/shared"

	"github.com/google/uuid"
)

type ChunkType int8

const (
	wal   ChunkType = 1
	btree ChunkType = 2
)

var (
	// maxCapacity the max capacity of a chunk
	maxCapacity int

	// dataDir the directory to place all data
	dataDir  string
	walDir   = "wal"
	btreeDir = "btree"

	// walSeq the sequence of wal chunk
	walSeq = uint64(0)

	// mutex
	mu sync.Mutex
)

func initWithConfig(cfg *shared.Config) {
	dataDir = cfg.Dir
	maxCapacity = cfg.ChunkConfig.MaxCapacity

	// list wal dataDir to look for latest wal seq
	files, err := ioutil.ReadDir(fmt.Sprintf("%s%s%s", dataDir, string(os.PathSeparator), chunkTypeFolder(wal)))
	if err != nil {
		// log critical error
	}
	// find last wal chunk file and get seq based in it's name
	if len(files) != 0 {
		file := files[len(files)-1]
		name, err := uuid.Parse(file.Name()[:36]) // uuid string length is 36
		if err != nil {
			// log critical error
		}
		walSeq = binary.BigEndian.Uint64(name[8:16]) + 1 // get and increase wal seq
	}
}

type chunk struct {
	// name of the chunk
	name uuid.UUID

	// chunkType the type of the chunk
	chunkType ChunkType

	// createTime the timestamp when the chunk is created
	createTime int64

	// sealedTime the timestamp when the chunk is sealed
	sealedTime int64

	// capacity max capacity of the chunk when allocated
	capacity int

	// sealed if the chunk is sealed and not allowed to change
	sealed bool

	// sealedLength the valid length of the chunk after sealed
	sealedLength int
}

func newChunk(chunkType ChunkType, cfg *shared.Config) (*chunk, error) {
	if walSeq == 0 {
		initWithConfig(cfg)
	}
	var name uuid.UUID
	if chunkType == btree {
		name = uuid.New()
	} else if chunkType == wal {
		mu.Lock()
		defer mu.Unlock()

		walSeq++ // increase wal seq
		name = newChunkNameWithSeq(walSeq)
	}
	_, err := os.Create(chunkFileName(name, chunkType))
	if err != nil {
		return nil, err
	}
	// Todo: write metadata for the chunk
	c := &chunk{
		name:       name,
		createTime: time.Now().UnixNano(),
		capacity:   maxCapacity,
		sealed:     false,
	}
	return c, nil
}

func newChunkNameWithSeq(seq uint64) uuid.UUID {
	name := uuid.New()
	binary.BigEndian.PutUint64(name[0:8:8], 0)
	binary.BigEndian.PutUint64(name[8:16:16], seq)
	return name
}

func chunkFileName(name uuid.UUID, chunkType ChunkType) string {
	return fmt.Sprintf("%s%s%s%s%s",
		dataDir, string(os.PathSeparator), chunkTypeFolder(chunkType), string(os.PathSeparator), name.String())
}

func chunkTypeFolder(chunkType ChunkType) string {
	var typeFolder string
	if chunkType == wal {
		typeFolder = walDir
	} else if chunkType == btree {
		typeFolder = btreeDir
	}
	return typeFolder
}
