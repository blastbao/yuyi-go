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

// Filter filter is used to do a quick check if the specified key already
// existed.
type Filter interface {
	// Put put the specified key in filter.
	Put(key *Key)

	// Remove remove the specified key from filter.
	Remove(key *Key)

	// SupportRemove if the filter support remove key from itself.
	SupportRemove() bool

	// MightContains if the specified key may already existed.
	MightContains(key *Key) bool
}

type CocoFilter struct {
}

type dummyFilter struct {
}

// Put put the specified key in filter.
func (filter *dummyFilter) Put(key *Key) {
	return
}

// Remove remove the specified key from filter.
func (filter *dummyFilter) Remove(key *Key) {
	return
}

// SupportRemove if the filter support remove key from itself.
func (filter *dummyFilter) SupportRemove() bool {
	return false
}

// MightContains if the specified key may already existed.
func (filter *dummyFilter) MightContains(key *Key) bool {
	return true
}
