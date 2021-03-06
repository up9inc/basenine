// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

import (
	"fmt"
	"sort"

	"github.com/dlclark/regexp2"
)

// addMacro takes macros map, macro and its corresponding expanded version
// as arguments. It stores the macro in a global map.
func AddMacro(macros map[string]string, macro string, expanded string) map[string]string {
	macros[macro] = fmt.Sprintf("(%s)", expanded)
	return macros
}

// expandMacro expands the macros in a given query, if there are any.
// It uses a lookahead regular expression to ignore the occurences
// of the macro inside the string literals.
func ExpandMacros(macros map[string]string, query string) (string, error) {
	var err error

	type pair struct {
		Macro    string
		Expanded string
	}

	var slice []pair
	for k, v := range macros {
		slice = append(slice, pair{k, v})
	}

	sort.Slice(slice, func(i, j int) bool {
		return len(slice[i].Macro) > len(slice[j].Macro)
	})

	for _, pair := range slice {
		regex := regexp2.MustCompile(fmt.Sprintf(`(%s)(?=(?:[^"]|"[^"]*")*$)`, pair.Macro), regexp2.None)
		query, err = regex.Replace(query, pair.Expanded, -1, -1)
		if err != nil {
			return query, err
		}
	}
	return query, nil
}
