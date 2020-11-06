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
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"yuyi-go/datastore/chunk"
)

var (
	minSeq = uint64(0)
	maxSeq = uint64(0xFFFFFFFFFFFFFFFF)

	// register all active datastore
	stores = make([]*DataStore, 0)

	done   = make(chan bool)
	ticker = time.NewTicker(5 * time.Second)
)

func init() {
	go background()
}

// background Check memory table flushing periodly
func background() {
	for {
		select {
		case <-done:
			ticker.Stop()
			return
		case <-ticker.C:
			for _, store := range stores {
				// check if the store should do flushing
				store.checkForFlushing()
			}
		}
	}
}

type DataStore struct {
	// name the name of the datastore
	name string

	// activeMemTable current using memory table for writing data to.
	activeMemTable MemTable

	// sealedMemTables the memory table instances that already reached
	//max limit size and need be persisted
	sealedMemTables []*MemTable

	// btree the structure for persisting data from memory table
	btree *BTree

	// mutations the channel to hold the mutations to be handled
	mutations chan mutation

	// committed the last sequence of write operation already persised. W
	// When read started, committed sequence will be acquired and make sure
	// that kv with uncommitted seq kv will be not read
	committed uint64

	// uncommitted the last sequence of write operation is doing persist.
	uncommitted uint64

	// walWriter the writer to handle wal writing
	walWriter chunk.ChunkWrtier

	// walWriterCallback the channel to handle wal write finished call back event
	walWriterCallback chan interface{}

	// ready if the datastore is ready
	ready bool

	// ready if the datastore is stopped
	stop bool

	// flushMutex the lock for checking if memory table is ready to flush
	flushMutex sync.Mutex
}

// New create a new datastore
func New() *DataStore {
	return &DataStore{}
}

func (store *DataStore) Put(key Key, value Value) error {
	// create mutation for this put operation
	entry := newKVEntry(key, value, Put)
	mutation := newMutation(entry)

	store.mutations <- mutation
	if mutation.wait() != nil {
		// put operation failed, need stop the datastore
		store.stop()
	}
}

func (store *DataStore) Remove(key Key) {
	return
}

func (store *DataStore) Has(key Key) (bool, error) {
	seq := store.committed
	value := store.activeMemTable.Get(key, seq)
	if value != nil {
		if value.Operation == Remove {
			return false, nil
		}
		return true, nil
	}
	for _, table := range store.sealedMemTables {
		value := table.Get(key, maxSeq)
		if value != nil {
			if value.Operation == Remove {
				return false, nil
			}
			return true, nil
		}
	}
	return store.btree.Has(&key)
}

func (store *DataStore) Get(key Key) (Value, error) {
	seq := store.committed
	value := store.activeMemTable.Get(key, seq)
	if value != nil {
		if value.Operation == Remove {
			return nil, nil
		}
		return value.Value, nil
	}
	for _, table := range store.sealedMemTables {
		value := table.Get(key, maxSeq)
		if value != nil {
			if value.Operation == Remove {
				return nil, nil
			}
			return value.Value, nil
		}
	}
	return store.btree.Get(&key)
}

func (store *DataStore) List(start Key, end Key, max int) []*KVPair {
	// seq := store.seq
	// activeIter := store.activeMemTable.List(start, end, seq)
	// sealedIter := make([]*memtable.Iterator, 0)
	// for _, memtable := range store.sealedMemTables {
	// 	sealedIter = append(sealedIter, memtable.List(start, end, seq))
	// }
	// resFromTree := store.btree.List(start, end, max)

	// res := make([]*memtable.KVPair, 0)
	// found := 0
	// Todo: implement min heap for merging result together
	return nil
}

func (store *DataStore) ReverseList(start Key, end Value, max uint16) []Value {
	return nil
}

func (store *DataStore) handleMutations() {
	for !store.stop {
		mutation := <-store.mutations
		// generate new seq and put to memory table
		mutation.entry.Seq = store.newSeq()
		store.putActive(mutation.entry)

		// write wal for this entry
		store.writeWal(mutation)
	}
}

func (store *DataStore) writeWal(m *mutation) {
	bytes := m.entry.buildBytes(store.name)
	store.walWriter.AsyncWrite()
}

func (store *DataStore) stop() {
	// handle datastore stop and clean up
}

var ErrTimeout = errors.New("Mutation timeout")

type mutation struct {
	// kv entry to put to datastore
	entry *KVEntry

	// complete the channel to notify task finished
	complete chan error

	// timeout the channel for writing timeout
	timeout <-chan time.Time
}

func newMutation(entry *KVEntry, duration time.Duration) *mutation {
	return &mutation{
		entry:    entry,
		complete: make(chan error),
		timeout:  time.After(duration),
	}
}

func (m *mutation) wait() {
	select {
	case err := <-task.complete:
		return err
	case <-task.timeout:
		return chunk.ErrTimeout
	}
}

func (store *DataStore) putEntry() {

}

func (store *DataStore) handleWalWriteCompleted() {
	for !store.stop {
		task := <-store.walWriter.CompletedTask(store.name)
		// generate new seq and put to active memory table
		newSeq := store.committed + 1
		task
	}
}

func (store *DataStore) execWriteTask(task *chunk.writeTask) error {
	// put the write task to the channel for writing
	select {
	case err := <-task.complete:
		return err
	case <-task.timeout:
		return chunk.ErrTimeout
	}
}

func (store *DataStore) newSeq() uint64 {
	for {
		oldSeq := store.uncommitted
		newSeq := oldSeq + 1
		if atomic.CompareAndSwapUint64(&store.uncommitted, oldSeq, newSeq) {
			return newSeq
		}
	}
}

func (store *DataStore) checkForFlushing() {
	store.flushMutex.Lock()
	defer store.flushMutex.Unlock()

	if store.shouldFlush() {
		go store.flush()
	}
}

func (store *DataStore) shouldFlush() bool {
	return len(store.sealedMemTables) != 0 && store.btree.isDumping()
}

func (store *DataStore) flush() {
	sealed := store.sealedMemTables[0:]

	dumper, err := newDumper(store.btree)
	if err != nil {
		// log error log
	}

	treeInfo, err := dumper.Dump(mergeMemTables(sealed))
	if err != nil {
		// log error log
	} else {
		store.mu.Lock()
		defer store.mu.Unlock()

		// update last tree info and
		store.btree.lastTreeInfo = treeInfo

		// release sealed memory table
		remaining := len(store.sealedMemTables) - len(sealed)
		store.sealedMemTables = store.sealedMemTables[0:remaining:remaining]
	}
	store.btree = nil
}

func mergeMemTables(tables []*MemTable) []*KVEntry {
	// new combined iterator
	iters := make([]*listIter, len(tables))
	for i, table := range tables {
		iters[i] = table.newMemTableIter(nil, nil, maxSeq)
	}

	combinedIter := newCombinedIter(iters)
	res := make([]*KVEntry, 0)
	for {
		if combinedIter.hasNext() {
			res = append(res, combinedIter.next())
		} else {
			break
		}
	}
	return res
}
