package main

import (
	"fmt"

	"github.com/dlclark/regexp2"
)

var macros map[string]string = make(map[string]string)

func addMacro(macro string, expanded string) {
	macros[macro] = fmt.Sprintf("(%s)", expanded)
}

func expandMacros(query string) (string, error) {
	var err error
	for macro, expanded := range macros {
		regex := regexp2.MustCompile(fmt.Sprintf(`(?<!["'])(%s)(?!["'])`, macro), regexp2.None)
		query, err = regex.Replace(query, expanded, -1, -1)
		if err != nil {
			return query, err
		}
	}
	return query, nil
}
