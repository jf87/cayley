// Copyright 2014 The Cayley Authors. All rights reserved.
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

package memstore

import (
	"errors"
	"fmt"
	"time"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/memstore/b" //NOTE imports the B tree package
	"github.com/google/cayley/quad"
)

const QuadStoreType = "memstore"

func init() {
	graph.RegisterQuadStore(QuadStoreType, false, func(string, graph.Options) (graph.QuadStore, error) {
		return newQuadStore(), nil
	}, nil, nil)
}

type QuadDirectionIndex struct {
	index [4]map[int64]*b.Tree // index is a slice of len 4 of a map from int to b.Tree
}

// Just initializes a new quad with empty map[int64]*b.Tree
func NewQuadDirectionIndex() QuadDirectionIndex {
	/*
		nq := QuadDirectionIndex{}
		fmt.Println(nq.index[0])
		nq = QuadDirectionIndex{}
		nq.index[0] = make(map[int64]*b.Tree)
		nq.index[1] = make(map[int64]*b.Tree)
		nq.index[2] = make(map[int64]*b.Tree)
		nq.index[3] = make(map[int64]*b.Tree)
		fmt.Println(nq.index[0])
		return nq
		//sayans["goku"] = 9001
		fmt.Println(quad.Subject - 1)
		fmt.Println(quad.Predicate - 1)
	*/
	return QuadDirectionIndex{[...]map[int64]*b.Tree{ // ... does not mean we need exactly 3 arguments, just a x number, if no is 0, then nil otherwise slice of type map with length of no of elements
		quad.Subject - 1:   make(map[int64]*b.Tree), // each line is one row
		quad.Predicate - 1: make(map[int64]*b.Tree), // we do -1 because Subject starts at 1
		quad.Object - 1:    make(map[int64]*b.Tree),
		quad.Label - 1:     make(map[int64]*b.Tree),
	}}
}

func (qdi QuadDirectionIndex) Tree(d quad.Direction, id int64) *b.Tree {
	fmt.Printf("QuadDirectionIndex\n")
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id]
	fmt.Printf("tree: %v \n", tree)
	fmt.Printf("ok: %v \n", ok)
	if !ok {
		// cmp() is in iterator.go
		// func cmp(a, b int64) int {
		// return int(a - b)
		// }
		tree = b.TreeNew(cmp)
		fmt.Printf("adding tree to quadrirection index d-1: %b id: %v \n", d-1, id)
		qdi.index[d-1][id] = tree
		fmt.Printf("qdi %v \n", qdi)
	}
	fmt.Printf("tree after: %v \n", tree)
	return tree
}

func (qdi QuadDirectionIndex) Get(d quad.Direction, id int64) (*b.Tree, bool) {
	fmt.Printf("GET  d-1: %b id: %v \n", d-1, id)
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id]
	fmt.Printf("GOT: %v\n", tree)
	return tree, ok
}

type LogEntry struct {
	ID        int64
	Quad      quad.Quad
	Action    graph.Procedure
	Timestamp time.Time
	DeletedBy int64
}

type QuadStore struct {
	nextID     int64
	nextQuadID int64
	idMap      map[string]int64
	revIDMap   map[int64]string
	log        []LogEntry
	size       int64
	index      QuadDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

func newQuadStore() *QuadStore {
	fmt.Println("Creating new in memory quadstore")
	return &QuadStore{
		idMap:    make(map[string]int64), // create a new empty map, from string to int
		revIDMap: make(map[int64]string),

		// Sentinel null entry so indices start at 1
		log: make([]LogEntry, 1, 200),

		index:      NewQuadDirectionIndex(),
		nextID:     1,
		nextQuadID: 1,
	}
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	for _, d := range deltas {
		var err error
		switch d.Action {
		case graph.Add:
			err = qs.AddDelta(d)
			if err != nil && ignoreOpts.IgnoreDup {
				err = nil
			}
		case graph.Delete:
			err = qs.RemoveDelta(d)
			if err != nil && ignoreOpts.IgnoreMissing {
				err = nil
			}
		default:
			err = errors.New("memstore: invalid action")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

const maxInt = int(^uint(0) >> 1)

func (qs *QuadStore) indexOf(t quad.Quad) (int64, bool) {
	fmt.Println("indexOf")
	min := maxInt
	var tree *b.Tree
	for d := quad.Subject; d <= quad.Label; d++ {
		sid := t.Get(d)
		if d == quad.Label && sid == "" {
			continue
		}
		id, ok := qs.idMap[sid]
		// If we've never heard about a node, it must not exist
		if !ok {
			return 0, false
		}
		index, ok := qs.index.Get(d, id)
		if !ok {
			// If it's never been indexed in this direction, it can't exist.
			return 0, false
		}
		if l := index.Len(); l < min {
			min, tree = l, index
		}
	}
	it := NewIterator(tree, "", qs)

	for it.Next() {
		val := it.Result()
		if t == qs.log[val.(int64)].Quad {
			return val.(int64), true
		}
	}
	return 0, false
}

// This is called to add a quad to the existing graph
func (qs *QuadStore) AddDelta(d graph.Delta) error {
	fmt.Printf("\n\nAddDelta\n")
	if _, exists := qs.indexOf(d.Quad); exists {
		return graph.ErrQuadExists
	}
	qid := qs.nextQuadID
	fmt.Printf("qid: %s \n", qid)
	qs.log = append(qs.log, LogEntry{
		ID:        d.ID.Int(),
		Quad:      d.Quad,
		Action:    d.Action,
		Timestamp: d.Timestamp})
	fmt.Printf("we have appended to qs.log: \n ID: %s \n Quad: %s \n Action: %s \n Timestamp: %s \n", d.ID.Int(), d.Quad, d.Action, d.Timestamp)
	qs.size++
	qs.nextQuadID++

	for dir := quad.Subject; dir <= quad.Label; dir++ {
		fmt.Printf("dir %s \n", dir)
		sid := d.Quad.Get(dir)
		fmt.Printf("sid %s \n", sid)
		if dir == quad.Label && sid == "" {
			fmt.Println("Label is empty, ignore")
			continue
		}
		if _, ok := qs.idMap[sid]; !ok {
			fmt.Println("putting into map /qs")
			qs.idMap[sid] = qs.nextID
			fmt.Printf("%s is stored in %v \n", sid, qs.idMap[sid])
			qs.revIDMap[qs.nextID] = sid
			qs.nextID++
		}
	}

	for dir := quad.Subject; dir <= quad.Label; dir++ {
		if dir == quad.Label && d.Quad.Get(dir) == "" {
			continue
		}
		fmt.Println("putting into tree")
		id := qs.idMap[d.Quad.Get(dir)]
		fmt.Printf("ID %v \n", id)   // id of this dir (if we have two labels witht he same name, we will have the same ID here!)
		fmt.Printf("DIR %v \n", dir) // subject, predicate, object, label
		fmt.Printf("QID %v \n", qid) // ID no of quad

		tree := qs.index.Tree(dir, id)
		tree.Set(qid, struct{}{})
		fmt.Printf("Tree %v \n", tree)
	}

	// TODO(barakmich): Add VIP indexing
	return nil
}

func (qs *QuadStore) RemoveDelta(d graph.Delta) error {
	prevQuadID, exists := qs.indexOf(d.Quad)
	if !exists {
		return graph.ErrQuadNotExist
	}

	quadID := qs.nextQuadID
	qs.log = append(qs.log, LogEntry{
		ID:        d.ID.Int(),
		Quad:      d.Quad,
		Action:    d.Action,
		Timestamp: d.Timestamp})
	qs.log[prevQuadID].DeletedBy = quadID
	qs.size--
	qs.nextQuadID++
	return nil
}

func (qs *QuadStore) Quad(index graph.Value) quad.Quad {
	return qs.log[index.(int64)].Quad
}

func (qs *QuadStore) QuadIterator(d quad.Direction, value graph.Value) graph.Iterator {
	index, ok := qs.index.Get(d, value.(int64))
	data := fmt.Sprintf("dir:%s val:%d", d, value.(int64))
	if ok {
		return NewIterator(index, data, qs)
	}
	return &iterator.Null{}
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.log[len(qs.log)-1].ID)
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) DebugPrint() {
	for i, l := range qs.log {
		if i == 0 {
			continue
		}
		glog.V(2).Infof("%d: %#v", i, l)
	}
}

func (qs *QuadStore) ValueOf(name string) graph.Value {
	return qs.idMap[name]
}

func (qs *QuadStore) NameOf(id graph.Value) string {
	return qs.revIDMap[id.(int64)]
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return newQuadsAllIterator(qs)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	name := qs.Quad(val).Get(d)
	return qs.ValueOf(name)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return newNodesAllIterator(qs)
}

func (qs *QuadStore) Close() {}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}
