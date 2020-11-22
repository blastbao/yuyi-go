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
	"yuyi-go/datastore/chunk"
	"yuyi-go/shared"

	"go.uber.org/zap"
)

type BTree struct {
	// lg the logger instance
	lg *zap.Logger

	// lastTreeInfo last tree info after Cow synced from memory table.
	lastTreeInfo *TreeInfo

	// dumper the dumper handler for persist new entries
	dumper *dumper

	// reader the reader utils to read pages from chunk
	reader chunk.ChunkReader

	// cfg the configuration instance
	cfg *shared.Config
}

func NewBTree(lg *zap.Logger, treeInfoYaml *TreeInfoYaml, cfg *shared.Config) (*BTree, error) {
	if lg == nil {
		lg = zap.NewNop()
	}

	reader, err := chunk.NewBtreeReader()
	if err != nil {
		return nil, err
	}
	// create last tree info
	var treeInfo *TreeInfo
	if treeInfoYaml != nil {
		rootAddr, err := treeInfoYaml.RootPage.Address()
		if err != nil {
			return nil, err
		}
		//		filterAddr, err := treeInfoYaml.filterPage.Address()
		//		if err != nil {
		//			return nil, err
		//		}
		content, err := reader.Read(rootAddr)
		if err != nil {
			return nil, err
		}
		rootPage := &page{
			content: content,
			entries: nil,
			addr:    rootAddr,
		}
		treeInfo = &TreeInfo{
			root:     rootPage,
			depth:    treeInfoYaml.TreeDepth,
			sequence: treeInfoYaml.Sequence,
		}
	}

	return &BTree{
		lg:           lg,
		lastTreeInfo: treeInfo,
		dumper:       nil,
		reader:       reader,
		cfg:          cfg,
	}, nil
}

type TreeInfo struct {
	// root the root page of the copy-on-write b+ tree.
	root *page

	// depth the depth of the tree
	depth int

	// filter the filter for quick filter key
	filter Filter

	// walSequence the wal sequence of the tree info
	walSequence uint64

	// sequence the sequence of the tree info
	sequence int
}

type pathItem struct {
	page  *page
	index int
}

type pathItemForDump struct {
	page  *pageForDump
	index int
}

func (tree *BTree) Has(key *Key) (bool, error) {
	treeInfo := tree.lastTreeInfo
	skipRead := !mightContains(treeInfo, key)
	if skipRead {
		return false, nil
	}

	// fetch the path with the specified key
	path, err := tree.findPath(treeInfo, key)
	if err != nil {
		return false, err
	}
	leaf := path[len(path)-1].page
	return leaf.Search(key) >= 0, nil
}

func (tree *BTree) Get(key *Key) (Value, error) {
	treeInfo := tree.lastTreeInfo
	skipRead := !mightContains(treeInfo, key)
	if skipRead {
		return nil, nil
	}

	// fetch the path with the specified key
	path, err := tree.findPath(treeInfo, key)
	if err != nil {
		return nil, err
	}
	leaf := path[len(path)-1].page
	index := leaf.Search(key)
	if index >= 0 {
		return leaf.Value(index), nil
	} else {
		return nil, nil
	}
}

type ListResult struct {
	pairs []*KVPair
	next  Key
}

func (r *ListResult) iterator() *ListResultIter {
	return &ListResultIter{
		res:   r,
		index: 0,
	}
}

type ListResultIter struct {
	res   *ListResult
	index int
}

func (iterator *ListResultIter) hasNext() bool {
	return iterator.index < len(iterator.res.pairs)
}

func (iterator *ListResultIter) next() *KVEntry {
	entry := &KVEntry{
		Key: iterator.res.pairs[iterator.index].Key,
		TableValue: TableValue{
			Operation: Put,
			Value:     iterator.res.pairs[iterator.index].Value,
		},
	}
	iterator.index++
	return entry
}

func (tree *BTree) List(start Key, end Key, max int) (*ListResult, error) {
	treeInfo := tree.lastTreeInfo
	return tree.list(treeInfo, start, end, max)
}

func (tree *BTree) list(treeInfo *TreeInfo, start Key, end Key, max int) (*ListResult, error) {
	if treeInfo == nil || treeInfo.root == nil || treeInfo.root.KVPairsCount() == 0 {
		return &ListResult{
			pairs: []*KVPair{},
			next:  nil,
		}, nil
	}

	res := make([]*KVPair, 0, max)
	if start == nil {
		start = []byte{}
	}

	path, err := tree.findPath(treeInfo, &start)
	if err != nil {
		return nil, err
	}
	leaf := path[len(path)-1].page
	pairsCount := leaf.KVPairsCount()
	if pairsCount == 0 {
		return &ListResult{make([]*KVPair, 0), nil}, nil
	}

	startIndex := leaf.Search(&start)
	if startIndex < 0 {
		startIndex = -startIndex - 1
	}

	keyFound := 0
	var next Key
outer:
	for {
		for i := startIndex; i < pairsCount; i++ {
			if end != nil && leaf.Key(i).Compare(end) >= 0 {
				break outer
			}
			if keyFound == max {
				// search for kv pairs finished, found next key
				next = leaf.Key(i)
				break outer
			}
			res = append(res, leaf.KVPair(i))
			keyFound++
		}
		// try find next leaf page
		path, err := tree.findNextLeaf(path)
		if err != nil {
			return nil, err
		}
		if path == nil {
			// reach end of the tree
			break
		}
		leaf = path[len(path)-1].page
		pairsCount = leaf.KVPairsCount()
		startIndex = 0
	}
	return &ListResult{res, next}, nil
}
func (tree *BTree) ReverseList(start Key, end Key, max int) []KVPair {
	return nil
}

// findPath find the path from root page to leaf page with the specified key
func (tree *BTree) findPath(treeInfo *TreeInfo, key *Key) ([]*pathItem, error) {
	root := treeInfo.root
	// create result slice and put root to the result
	res := make([]*pathItem, treeInfo.depth, treeInfo.depth)
	res[0] = &pathItem{root, 0}

	parent := root
	depth := 1
	for {
		index := parent.Search(key)
		if index < 0 {
			if index == -1 {
				index = 0
			} else {
				index = -index - 2
			}
		}
		// find child page with the found index and push it in result.
		childAddress := parent.ChildAddress(index)
		childPage, err := tree.readPage(childAddress)
		if err != nil {
			return nil, err
		}
		res[depth] = &pathItem{childPage, index}

		if childPage.Type() == Leaf {
			// found leaf page, search end
			break
		} else {
			parent = childPage
			depth++
		}
	}
	return res, nil
}

func (tree *BTree) findNextLeaf(curPath []*pathItem) ([]*pathItem, error) {
	depth := len(curPath)
	// recusive find next index
	i := depth - 1
	for ; i > 0; i-- {
		if curPath[i].index+1 < curPath[i-1].page.KVPairsCount() {
			nextLeaf, err := tree.readPage(curPath[i-1].page.ChildAddress(curPath[i].index + 1))
			if err != nil {
				return nil, err
			}
			curPath[i] = &pathItem{nextLeaf, curPath[i].index + 1}
			break
		} else {
			curPath[i] = nil
		}
	}
	if curPath[1] == nil {
		// reach end of the tree
		return nil, nil
	}
	for {
		if curPath[i].page.Type() == Leaf {
			break
		}
		// find child page with the found index and push it in result.
		childAddress := curPath[i].page.ChildAddress(0)
		childPage, err := tree.readPage(childAddress)
		if err != nil {
			return nil, err
		}
		curPath[i+1] = &pathItem{childPage, 0}
		i++
	}
	return curPath, nil
}

func (tree *BTree) readPage(addr chunk.Address) (*page, error) {
	content, err := tree.reader.Read(addr)
	if err != nil {
		return nil, err
	}
	return &page{
		content: content,
		entries: nil,
		addr:    addr,
	}, nil
}

func (tree *BTree) isDumping() bool {
	return tree.dumper != nil
}

func mightContains(treeInfo *TreeInfo, key *Key) bool {
	if treeInfo == nil || !treeInfo.filter.MightContains(key) {
		return false
	}
	return true
}
