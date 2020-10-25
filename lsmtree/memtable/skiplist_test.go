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

package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
)

func TestPutToSkipListSingleThread(t *testing.T) {
	// generate key value pairs to put
	count := 100000
	pairs := make([]*KVPair, count)
	for i := 0; i < count; i++ {
		pairs[i] = &KVPair{
			Key:   randomBytes(100, defaultLetters),
			Value: randomBytes(200, defaultLetters),
		}
	}

	skipList := NewSkipList()
	for i, pair := range pairs {
		err := skipList.Put(&KVEntry{
			Key: pair.Key,
			TableValue: TableValue{
				Operation: Put,
				Value:     pair.Value,
			},
			Seq: uint64(i),
		})
		if err != nil {
			t.Error("Put entry failed", pair.Key)
			return
		}
	}

	verifyPutResult(t, skipList, pairs)
}

func TestPutToSkipListMiltyThread(t *testing.T) {
	routines := 16
	runtime.GOMAXPROCS(routines)

	skipList := NewSkipList()
	// generate key value pairs to put
	count := 100000
	pairs := make([]*KVPair, count)
	for i := 0; i < count; i++ {
		pairs[i] = &KVPair{
			Key:   randomBytes(100, defaultLetters),
			Value: randomBytes(200, defaultLetters),
		}
	}

	var wg sync.WaitGroup
	wg.Add(routines)
	for num := 0; num < routines; num++ {
		index := num
		go func() {
			defer wg.Done()

			fmt.Printf("Start put in goroutine %d\n", index)
			for i, pair := range pairs {
				if i%routines != index {
					continue
				}

				err := skipList.Put(&KVEntry{
					Key: pair.Key,
					TableValue: TableValue{
						Operation: Put,
						Value:     pair.Value,
					},
					Seq: uint64(i),
				})
				if err != nil {
					t.Error("Put entry failed", pair.Key)
					return
				}
			}
			fmt.Printf("Finish put in goroutine %d\n", index)
		}()
	}
	wg.Wait()

	verifyPutResult(t, skipList, pairs)
}

func verifyPutResult(t *testing.T, skipList *SkipList, pairs []*KVPair) {
	fmt.Println("Verify the put result")

	// iterate pairs to verify the put
	for i, pair := range pairs {
		v := skipList.Get(pair.Key, uint64(i))
		if bytes.Compare(pair.Value, v.Value) != 0 {
			t.Error("Failed to get value with key ", pair.Key)
			break
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) <= 0
	})
	i := 0
	iterator := skipList.NewIterator(nil, nil, 0)
	for {
		if iterator.hasNext() {
			entry := iterator.next()
			if entry.Key.Compare(pairs[i].Key) != 0 {
				t.Error("Failed to iterator skip list")
				break
			}
			i++
		} else {
			break
		}
	}
	if i != len(pairs) {
		t.Error("Failed to iterator skip list")
	}
}

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
func randomBytes(n int, allowedChars ...[]rune) []byte {
	var letters []rune

	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return []byte(string(b))
}
