// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package main

import (
	"sort"

	jp "github.com/ohler55/ojg/jp"
)

// addIndex adds the indexed path into a global slice.
func addIndex(path string) (err error) {
	cs.RLock()
	indexes := cs.indexes
	cs.RUnlock()

	if strContains(indexes, path) {
		return
	}

	var indexedPath jp.Expr
	indexedPath, err = jp.ParseString(path)
	if err != nil {
		return
	}

	cs.Lock()
	cs.indexes = append(cs.indexes, path)
	cs.indexedPaths = append(cs.indexedPaths, indexedPath)
	cs.Unlock()

	return
}

// handleIndexedInsertion updates and sorts the indexed JSONPaths.
// Expects int data type. Should be called with a lock.
func handleIndexedInsertion(d map[string]interface{}, offset int64) {
	for i, indexedPath := range cs.indexedPaths {
		result := indexedPath.Get(d)

		if len(result) > 0 {
			v := float64Operand(result[0])
			cs.indexedValues[i] = append(cs.indexedValues[i], IndexedValue{
				Real:   v,
				Offset: offset,
			})

			sort.Slice(cs.indexedValues, func(j, k int) bool {
				return cs.indexedValues[i][j].Real < cs.indexedValues[i][k].Real
			})
		}
	}
}

// computeQueryJump computes the queryJump value of given path with a fast iteration
// if the path is indexed. Otherwise it returns 0
func computeQueryJump(path string, qvd QueryValDirection) QueryJump {
	cs.RLock()
	indexes := cs.indexes
	cs.RUnlock()

	for i, indexedPath := range indexes {
		if indexedPath == path {
			cs.RLock()
			index := cs.indexedValues[i]
			cs.RUnlock()

			j := sort.Search(len(index), func(k int) bool {
				value := qvd.value
				real := index[k].Real
				if qvd.operator == "==" {
					return value == real
				} else {
					return comparisonOperations[qvd.operator].(func(interface{}, interface{}) bool)(value, real)
				}
			})

			return QueryJump{
				offset: index[j].Offset,
				qvd:    qvd,
			}
		}
	}

	return QueryJump{}
}
