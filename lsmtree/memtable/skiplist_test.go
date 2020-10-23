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
	"math/rand"
	"testing"
)

func TestPutToSkipListSingleThread(t *testing.T) {
	// generate key value pairs to put
	pairs := make([]*KVPair, 10000)
	for i := 0; i < 10000; i++ {
		pairs[i] = &KVPair{
			Key:   randomBytes(100, defaultLetters),
			Value: randomBytes(200, defaultLetters),
		}
	}

	skipList := SkipList{}
	for i, pair := range pairs {
		skipList.Put(pair.Key, pair.Value, i)
	}

	// iterate pairs to verify the put
	for i, pair := range pairs {
		v := skipList.Get(pair.Key, i)
		if bytes.Compare(pair.Value, v) != 0 {
			t.Error("Failed to get value with key ", pair.Key)
			break
		}
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
