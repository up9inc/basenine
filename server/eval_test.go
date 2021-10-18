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
	{`true and 5`, `{}`, true},
	{`false and 5`, `{}`, false},
	{`true and 0`, `{}`, false},
	{`5 == 5`, `{}`, true},
	{`3 == 5`, `{}`, false},
	{`"abc" == "abc"`, `{}`, true},
	{`"abc" == "xyz"`, `{}`, false},
	{`"abc" != "xyz"`, `{}`, true},
	{`"abc" != "abc"`, `{}`, false},
	{`true == true`, `{}`, true},
	{`true != true`, `{}`, false},
	{`true == false`, `{}`, false},
	{`3.14 == 3.14`, `{}`, true},
	{`3.14 == 42`, `{}`, false},
}

func TestEval(t *testing.T) {
	for _, row := range data {
		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}
		truth, err := Eval(expr, row.json)
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
