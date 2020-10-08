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

type MemTable struct {
	sealed   bool
	capacity int32
}

func (t *MemTable) Contains(key *Key) bool {
	return false
}

func (t *MemTable) Has(key *Key) bool {
	return false
}

func (t *MemTable) GetIfExists(key *Key) Value {
	return nil
}
