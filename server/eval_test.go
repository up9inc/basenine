package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var data = []struct {
	query string
	json  string
	truth bool
}{
	{`true and true`, `{}`, true},
	{`true and false`, `{}`, false},
	{`false and true`, `{}`, false},
	{`false and false`, `{}`, false},
	{`true or true`, `{}`, true},
	{`false or true`, `{}`, true},
	{`true or false`, `{}`, true},
	{`false or false`, `{}`, false},
}

func TestEval(t *testing.T) {
	for _, row := range data {
		truth, err := Eval(Parse(row.query), row.json)
		if err != nil {
			t.Fatal(err.Error())
		}
		if row.truth {
			assert.True(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, row.json))
		} else {
			assert.False(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, row.json))
		}
	}
}
