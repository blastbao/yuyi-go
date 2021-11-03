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
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/blastbao/yuyi-go/datastore/chunk"
	"github.com/blastbao/yuyi-go/shared"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	minSeq = uint64(0)
	maxSeq = uint64(0xFFFFFFFFFFFFFFFF)

	// register all active datastore
	regMux sync.Mutex
	stores = make([]*DataStore, 0)

	done   = make(chan bool)
	ticker = time.NewTicker(1 * time.Second)

	pathSeparator = string(os.PathSeparator)

	treeRecordDir = "treerecord"
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
	// lg the logger instance
	lg *zap.Logger

	// name the name of the datastore
	name uuid.UUID

	// nameString the name string format of the datastore
	nameString string

	// memTableMaxCapacity the max capacity of a memory table
	memTableMaxCapacity int

	// activeMemTable current using memory table for writing data to.
	activeMemTable *MemTable

	// sealedMemTables the memory table instances that already reached
	//max limit size and need be persisted
	sealedMemTables []*MemTable

	// btree the structure for persisting data from memory table
	btree *BTree

	// mutations the channel to hold the mutations to be handled
	mutations chan *mutation

	// committed the last sequence of write operation already persised. W
	// When read started, committed sequence will be acquired and make sure
	// that kv with uncommitted seq kv will be not read
	committed uint64

	// uncommitted the last sequence of write operation is doing persist.
	uncommitted uint64

	// walWriter the writer to handle wal writing
	walWriter *chunk.WalWriter

	// ready if the datastore is ready
	ready bool

	// ready if the datastore is stopped
	stopped bool

	// mtMu the lock for change active and sealed memory table
	mtMu sync.Mutex

	// flushMu the lock for checking if memory table is ready to flush
	flushMu sync.Mutex

	// flushing the flag to mark the store is flushing memory tables to btree
	flushing bool

	// cfg the configuration instance
	cfg *shared.Config
}

// New create a new datastore
func New(lg *zap.Logger, name uuid.UUID, cfg *shared.Config) (*DataStore, error) {
	if lg == nil {
		lg = zap.NewNop()
	}

	// read last tree record
	treeRecord, err := getLastTreeRecord(name.String(), cfg)
	if err != nil {
		return nil, err
	}

	// initialize btree
	var btree *BTree
	if treeRecord != nil {
		btree, err = NewBTree(lg, &treeRecord.TreeInfo, cfg)
	} else {
		btree, err = NewBTree(lg, nil, cfg)
	}
	if err != nil {
		return nil, err
	}

	// initialize wal writer
	walWriter, err := chunk.NewWalWriter(cfg)
	if err != nil {
		return nil, err
	}

	var walSeq uint64
	if treeRecord != nil {
		walSeq = treeRecord.WalSequence
	}
	// create dataStore instance
	datastore := &DataStore{
		lg:                  lg,
		name:                name,
		nameString:          name.String(),
		memTableMaxCapacity: 512 * 1024,
		activeMemTable:      NewMemTable(),
		sealedMemTables:     make([]*MemTable, 0),
		btree:               btree,
		mutations:           make(chan *mutation),
		committed:           walSeq,
		uncommitted:         walSeq,
		walWriter:           walWriter,
		ready:               true,
		stopped:             false,
		flushing:            false,
		cfg:                 cfg,
	}
	datastore.initBackground()

	// replay wal
	if treeRecord == nil {
		datastore.replayWal(1, 0)
	} else {
		datastore.replayWal(treeRecord.WalChunkSequence, treeRecord.WalEndOffset)
	}
	return datastore, nil
}

type TreeRecordYaml struct {
	// tree info related
	TreeInfo TreeInfoYaml `yaml:"tree-info"`

	// wal related
	WalSequence      uint64 `yaml:"wal-sequence"`
	WalChunkSequence uint64 `yaml:"wal-chunk-sequence"`
	WalEndOffset     int    `yaml:"wal-end-offset"`
}

type TreeInfoYaml struct {
	Sequence   int      `yaml:"sequence"`
	RootPage   PageYaml `yaml:"root-page"`
	FilterPage PageYaml `yaml:"filter-page"`
	DeltaPage  PageYaml `yaml:"delta-page"`
	TreeDepth  int      `yaml:"treeDepth"`
}

type PageYaml struct {
	Chunk  string `yaml:"chunk"`
	Offset int    `yaml:"offset"`
	Length int    `yaml:"length"`
}

func (p *PageYaml) Address() (chunk.Address, error) {
	chunkName, err := uuid.Parse(p.Chunk)
	if err != nil {
		return chunk.Address{}, err
	}
	return chunk.NewAddress(chunkName, p.Offset, p.Length), nil
}

var errNoValidTreeRecordFound = errors.New("No valid tree record found")

func getLastTreeRecord(storeName string, cfg *shared.Config) (*TreeRecordYaml, error) {
	// list tree record dir to get lastest tree record file
	files, err := ioutil.ReadDir(treeRecordAbsoluteDir(storeName, cfg))
	if err != nil {
		// log critical error
	}
	// find last wal chunk file and get seq based in it's name
	if len(files) != 0 {
		var treeRecord TreeRecordYaml
		for i := len(files) - 1; i >= 0; i-- {
			content, err := ioutil.ReadFile(treeRecordAbsoluteDir(storeName, cfg) + pathSeparator + files[i].Name())
			if err != nil {
				return nil, err
			}
			treeRecord = TreeRecordYaml{}
			err = yaml.Unmarshal(content, &treeRecord)
			if err != nil {
				// log critical error
				continue
			}
			break
		}
		if (treeRecord == TreeRecordYaml{}) {
			// failed to find valid tree record
			// log critical error
			return nil, errNoValidTreeRecordFound
		}
		return &treeRecord, nil
	}
	// not tree record found
	return nil, nil
}

func (store *DataStore) initBackground() {
	// start a goroutine to handle mutations' writing
	go store.handleMutations()

	// register store
	regMux.Lock()
	defer regMux.Unlock()
	stores = append(stores, store)
}

func (store *DataStore) replayWal(seq uint64, offset int) error {
	replayer, err := chunk.NewWalReader(store.name, seq, offset)
	if err != nil {
		return err
	}

	complete := make(chan error, 10)
	blockChan := replayer.Replay(complete)
	for {
		select {
		case block, more := <-blockChan:
			if more {
				// Todo: active memory table's last wal address should be updated here
				store.putActive(parseKVEntryBytes(block))
				store.committed++
				store.uncommitted++
			} else {
				// log finished
				return nil
			}

		case err := <-complete:
			return err
		}
	}
}

var ErrMutationFailed = errors.New("DataStore mutation failed")

func (store *DataStore) Put(key Key, value Value) error {
	// create mutation for this put operation
	entry := newKVEntry(key, value, Put)
	mutation := newMutation(entry, 10*time.Second)

	store.mutations <- mutation
	err := mutation.wait()
	if err != nil {
		// put operation failed, need stop the datastore
		store.stop()
		return ErrMutationFailed
	}
	return err
}

func (store *DataStore) Remove(key Key) error {
	// create mutation for this put operation
	entry := newKVEntry(key, nil, Remove)
	mutation := newMutation(entry, 10*time.Second)

	store.mutations <- mutation
	err := mutation.wait()
	if err != nil {
		// put operation failed, need stop the datastore
		store.stop()
		return ErrMutationFailed
	}
	return err
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

func (store *DataStore) List(start Key, end Key, max int) (*ListResult, error) {
	var iterator iter
	var next Key
	for {
		iters := make([]iter, 0)
		committed := store.committed

		iters = append(iters, store.activeMemTable.List(start, end, committed))
		for _, memtable := range store.sealedMemTables {
			iters = append(iters, memtable.List(start, end, committed))
		}
		treeInfo := store.btree.lastTreeInfo
		if treeInfo != nil && treeInfo.walSequence > committed {
			// current tree contains uncommitted content, retry list
			continue
		}
		resFromTree, err := store.btree.list(treeInfo, start, end, max)
		if err != nil {
			// log error here
			return nil, err
		}
		next = resFromTree.next
		iters = append(iters, resFromTree.iterator())

		iterator = newCombinedIter(iters...)
		break
	}

	res := make([]*KVPair, 0)
	found := 0
	for {
		if iterator.hasNext() && found < max {
			entry := iterator.next()
			res = append(res, &KVPair{
				Key:   entry.Key,
				Value: entry.TableValue.Value,
			})
			found++
			continue
		}
		break
	}
	// prepare next key of the list result
	if iterator.hasNext() {
		entry := iterator.next()
		compareRes := entry.Key.Compare(next)
		if next == nil || compareRes <= 0 {
			next = entry.Key
		}
	}
	return &ListResult{
		pairs: res,
		next:  next,
	}, nil
}

func (store *DataStore) ReverseList(start Key, end Value, max uint16) (*ListResult, error) {
	return nil, nil
}

func (store *DataStore) handleMutations() {
	for !store.stopped {
		mutation := <-store.mutations
		// generate new seq and put to memory table
		mutation.entry.Seq = store.newSeq()
		store.putActive(mutation.entry)

		// async write wal for this entry.
		bytes := mutation.entry.buildBytes(store.name)

		// 
		store.walWriter.AsyncWrite(bytes, mutation.complete, store.asyncWriteCallback)
	}
}

func (store *DataStore) putActive(entry *KVEntry) {
	store.mtMu.Lock()
	defer store.mtMu.Unlock()
	size := entry.size()
	for store.activeMemTable.capacity+size > store.memTableMaxCapacity {

		// double check if need seal current active
		if store.activeMemTable.capacity+size > store.memTableMaxCapacity {
			// need seal current active memory table and create a new one
			store.activeMemTable.sealed = true
			sealed := make([]*MemTable, 0)
			sealed = append(sealed, store.activeMemTable)
			sealed = append(sealed, store.sealedMemTables...)
			store.sealedMemTables = sealed

			store.activeMemTable = NewMemTable()
		}
	}
	store.activeMemTable.Put(entry)
}

func (store *DataStore) asyncWriteCallback(addr chunk.Address, err error) {
	if err != nil {
		store.stop()
		return
	}
	// update wal address in memory table and increase committed sequence
	store.updateWalAddr(addr)
	store.committed++
}

var ErrUpdateWalAddr = errors.New("Update wal addr for memory table failed")

func (store *DataStore) updateWalAddr(addr chunk.Address) error {
	seq := store.committed + 1
	for {
		active := store.activeMemTable
		// check if update active wal address first
		if seq <= active.lastSeq && seq >= active.firstSeq {
			active.lastWalAddr = addr
			return nil
		}

		// check if update sealed wal addr
		if seq < active.lastSeq {
			break
		}
	}

	// check if update the addr to sealed memory table
	for _, sealed := range store.sealedMemTables {
		if seq <= sealed.lastSeq && seq >= sealed.firstSeq {
			sealed.lastWalAddr = addr
			return nil
		}
	}
	return ErrUpdateWalAddr
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
		complete: make(chan error, 1), // channel with buffer 1 to avoid blocking callback
		timeout:  time.After(duration),
	}
}

func (m *mutation) wait() error {
	select {
	case err := <-m.complete:
		return err
	case <-m.timeout:
		return ErrTimeout
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
	store.flushMu.Lock()
	defer store.flushMu.Unlock()

	if store.shouldFlush() {
		store.markFlushing()
		go store.flush()
	}
}

func (store *DataStore) shouldFlush() bool {
	return len(store.sealedMemTables) != 0 && !store.flushing
}

func (store *DataStore) flush() {
	defer store.unmarkFlushing()
	sealedMemTables := store.sealedMemTables[0:]

	// check if sealed memory table's last wal have been updated
	for i, table := range sealedMemTables {
		if table.lastSeq < store.committed {
			// all remaining tables are ready to flush
			if i != 0 {
				sealedMemTables = sealedMemTables[i:]
				break
			}
		}
	}

	dumper, err := newDumper(store.lg, store.btree, store.cfg)
	if err != nil {
		// log error log
		return
	}

	treeInfo, err := dumper.Dump(mergeMemTables(sealedMemTables), sealedMemTables[0].lastSeq)
	if err != nil {
		// log error log
	} else {
		store.mtMu.Lock()
		defer store.mtMu.Unlock()

		// update last tree info and
		store.btree.lastTreeInfo = treeInfo

		// release sealed memory table
		remaining := len(store.sealedMemTables) - len(sealedMemTables)
		store.sealedMemTables = store.sealedMemTables[0:remaining:remaining]

		// sync tree info
		store.syncTreeRecord(sealedMemTables[0].lastSeq, sealedMemTables[0].lastWalAddr)
	}
	store.btree.dumper = nil
}

func (store *DataStore) syncTreeRecord(walSeq uint64, walAddr chunk.Address) {
	rootAddr := store.btree.lastTreeInfo.root.addr
	rootYaml := PageYaml{
		Chunk:  rootAddr.Chunk.String(),
		Offset: rootAddr.Offset,
		Length: rootAddr.Length,
	}
	treeInfoYaml := TreeInfoYaml{
		Sequence:  store.btree.lastTreeInfo.sequence,
		RootPage:  rootYaml,
		TreeDepth: store.btree.lastTreeInfo.depth,
	}
	treeRecordYaml := TreeRecordYaml{
		TreeInfo:         treeInfoYaml,
		WalSequence:      walSeq,
		WalChunkSequence: binary.BigEndian.Uint64(walAddr.Chunk[8:16:16]),
		WalEndOffset:     walAddr.Offset + walAddr.Length,
	}
	out, err := yaml.Marshal(&treeRecordYaml)
	if err != nil {
		// log err here
		return
	}

	dir := treeRecordAbsoluteDir(store.nameString, store.cfg)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, os.ModePerm)
	}
	fPath := fmt.Sprintf("%s%s%016d.yaml", dir, pathSeparator, store.btree.lastTreeInfo.sequence)
	var f *os.File
	f, err = os.Create(fPath)
	if err != nil {
		// log err here
		return
	}
	defer f.Close()
	_, err = f.Write(out)
	if err != nil {
		// log err here
		return
	}
}

func treeRecordAbsoluteDir(storeName string, cfg *shared.Config) string {
	return fmt.Sprintf("%s%s%s%s%s", cfg.Dir, pathSeparator, treeRecordDir, pathSeparator, storeName)
}

func (store *DataStore) markFlushing() {
	store.flushing = true
}

func (store *DataStore) unmarkFlushing() {
	store.flushing = false
}

func mergeMemTables(tables []*MemTable) []*KVEntry {
	// new combined iterator
	iters := make([]iter, len(tables))
	for i, table := range tables {
		iters[i] = table.newMemTableIter(nil, nil, maxSeq)
	}

	combinedIter := newCombinedIter(iters...)
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
