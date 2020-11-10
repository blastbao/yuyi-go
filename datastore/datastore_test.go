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

package datastore

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPutEntries(t *testing.T) {
	cpu := runtime.NumCPU()
	runtime.GOMAXPROCS(cpu)

	datastore, err := New()
	if err != nil {
		t.Error("datastore create failed")
		return
	}
	var wg sync.WaitGroup
	allEntries := make([][]*KVEntry, cpu)
	for i := 0; i < cpu; i++ {
		entries := randomPutKVEntries(2000)
		allEntries[i] = entries
	}
	for i := 0; i < cpu; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			fmt.Printf("Goroutine %d\n", j)

			for j, entry := range allEntries[j] {
				fmt.Printf("Put key %d\n", j)
				datastore.Put(entry.Key, entry.TableValue.Value)
			}
		}(i)
	}

	wg.Wait()

	time.Sleep(30 * time.Second)
	fmt.Println("Finished Put Test")
}
