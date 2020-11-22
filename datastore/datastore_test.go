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
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
	"yuyi-go/datastore/chunk"
	"yuyi-go/shared"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestPutEntries(t *testing.T) {
	cfg, err := setupCfg()
	if err != nil {
		t.Error("Failed to initialize config")
		return
	}

	name := uuid.New()
	datastore, err := New(zap.NewExample(), name, cfg)
	if err != nil {
		t.Error("datastore create failed")
		return
	}
	allEntries, err := preparePutEntries(datastore, t)
	if err != nil {
		t.Error("put entries failed")
		return
	}

	// wait tree dumper finished
	time.Sleep(30 * time.Second)

	// init datastore with latest tree record
	datastore, err = New(zap.NewExample(), name, cfg)
	if err != nil {
		t.Error("datastore create failed")
		return
	}

	index := 0
	// do list
	var start Key
outer:
	for {
		listRes, err := datastore.List(start, nil, 1000)
		if err != nil {
			t.Error("btree list failed")
			break outer
		}
		for _, pair := range listRes.pairs {
			if bytes.Compare(allEntries[index].Key, pair.Key) != 0 {
				t.Error("key invalid", "\n", allEntries[index].Key, "\n", pair.Key)
				break outer
			}
			index++
		}
		start = listRes.next
		if start == nil {
			break
		}
	}
	fmt.Println("Finished Put Test")
}

func TestWalReplayer(t *testing.T) {
	cfg, err := setupCfg()
	if err != nil {
		t.Error("Failed to initialize config")
		return
	}

	name := uuid.New()
	datastore, err := New(zap.NewExample(), name, cfg)
	if err != nil {
		t.Error("datastore create failed")
		return
	}
	_, err = preparePutEntries(datastore, t)
	if err != nil {
		t.Error("put entries failed")
		return
	}
	// wait all wal write synced
	time.Sleep(1 * time.Second)

	if err != nil {
		t.Error("datastore create failed")
		return
	}
	replayer, err := chunk.NewWalReader(datastore.name, 1, 0)
	if err != nil {
		t.Error("create replayer failed")
		return
	}

	var count int
	complete := make(chan error, 1)
	blockChan := replayer.Replay(complete)
	for {
		select {
		case <-blockChan:
			count++
		case err := <-complete:
			if err != nil {
				t.Error("put entries failed")
			}
			if count != runtime.NumCPU()*2000 {
				t.Error("Total count mismatch")
			}
			return
		}
	}
}

func setupCfg() (*shared.Config, error) {
	cfg, err := shared.NewConfig("../yuyi.config.yaml")
	if err != nil {
		return nil, err
	}
	cfg.Dir = "../tmp" // set dir to tmp

	dataDir := cfg.Dir
	err = os.RemoveAll(dataDir)
	if err != nil {
		return nil, err
	}

	// prepare data dir
	err = os.Mkdir(dataDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// prepare wal dir
	err = os.Mkdir(dataDir+"/wal", os.ModePerm)
	if err != nil {
		return nil, err
	}

	// prepare btree dir
	err = os.Mkdir(dataDir+"/btree", os.ModePerm)
	if err != nil {
		return nil, err
	}

	// prepare tree info
	err = os.Mkdir(dataDir+"/treerecord", os.ModePerm)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func preparePutEntries(store *DataStore, t *testing.T) ([]*KVEntry, error) {
	cpu := runtime.NumCPU()
	runtime.GOMAXPROCS(cpu)

	var wg sync.WaitGroup
	res := make([]*KVEntry, 0)
	allEntries := make([][]*KVEntry, cpu)
	for i := 0; i < cpu; i++ {
		entries := randomPutKVEntries(2000)
		res = mergeEntries(res, entries)
		allEntries[i] = entries
	}
	for i := 0; i < cpu; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			fmt.Printf("Goroutine %d\n", j)

			for j, entry := range allEntries[j] {
				fmt.Printf("Put key %d\n", j)
				store.Put(entry.Key, entry.TableValue.Value)
			}
		}(i)
	}
	wg.Wait()
	return res, nil
}
