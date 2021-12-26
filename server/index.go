// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package main

// addIndex adds the indexed path into a global slice.
func addIndex(path string) {
	cs.RLock()
	indexes := cs.indexes
	cs.RUnlock()

	if strContains(indexes, path) {
		return
	}

	cs.Lock()
	cs.indexes = append(cs.indexes, path)
	cs.Unlock()
}
