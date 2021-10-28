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
	limit uint64
}{
	{`true and true`, `{}`, true, 0},
	{`true and false`, `{}`, false, 0},
	{`false and true`, `{}`, false, 0},
	{`false and false`, `{}`, false, 0},
	{`true or true`, `{}`, true, 0},
	{`false or true`, `{}`, true, 0},
	{`true or false`, `{}`, true, 0},
	{`false or false`, `{}`, false, 0},
	{`true and 5`, `{}`, true, 0},
	{`false and 5`, `{}`, false, 0},
	{`true and 0`, `{}`, false, 0},
	{`5 == 5`, `{}`, true, 0},
	{`3 == 5`, `{}`, false, 0},
	{`"abc" == "abc"`, `{}`, true, 0},
	{`"abc" == "xyz"`, `{}`, false, 0},
	{`"abc" != "xyz"`, `{}`, true, 0},
	{`"abc" != "abc"`, `{}`, false, 0},
	{`true == true`, `{}`, true, 0},
	{`true != true`, `{}`, false, 0},
	{`true == false`, `{}`, false, 0},
	{`3.14 == 3.14`, `{}`, true, 0},
	{`3.14 == 42`, `{}`, false, 0},
	{`42 > 41`, `{}`, true, 0},
	{`42 >= 42`, `{}`, true, 0},
	{`41 >= 42`, `{}`, false, 0},
	{`13 < 42`, `{}`, true, 0},
	{`42 < 13`, `{}`, false, 0},
	{`!true`, `{}`, false, 0},
	{`-300 < 42`, `{}`, true, 0},
	{`true and !(5 == a)`, `{"a": 4}`, true, 0},
	{`true and !(5 == a)`, `{"a": 5}`, false, 0},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"hello"},"x":{"y":3.15}}`, true, 0},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"hello"},"x":{"y":3.13}}`, false, 0},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"mello"},"x":{"y":3.15}}`, false, 0},
	{`brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand.name != "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.game == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name == r"Chev.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand.name != r"Chev.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name == r"Bug.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name != r"Bug.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`request.path[1] == "v1"`, `{"request":{"path":["api","v1","example"]}}`, true, 0},
	{`request.path[1] != "v1"`, `{"request":{"path":["api","v1","example"]}}`, false, 0},
	{`request.headers["a"] == "b"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, true, 0},
	{`request.headers["a"] != "b"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, false, 0},
	{`request.headers["a"] == "d"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, false, 0},
	{`request.headers["e"].x == "y"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, true, 0},
	{`request.headers["e"].x == "z"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, false, 0},
	{`request.headers["e"].x != "y"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, false, 0},
	{`request.headers["e"].x != "z"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, true, 0},
	{`brand.name.startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand.name.startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name.endsWith("let")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand.name.endsWith("le")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name.contains("ro")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand.name.contains("hello")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand["name"].startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand["name"].startsWithx("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand["name"].startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`timestamp > datetime("10/19/2021, 6:29:02 PM")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, true, 0},
	{`timestamp > datetime("10/19/2021, 7:29:02 PM")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, false, 0},
	{`request.headers["a"] == "b" and request.path[1] == "v1"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, true, 0},
	{`year == salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`, true, 0},
	{`year == salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, false, 0},
	{`year != salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, true, 0},
	{`year != salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`, false, 0},
	{`year > salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, true, 0},
	{`year > salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2022}`, false, 0},
	{`brand.name == "Chevrolet" and year == 2021`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`brand.name == "Chevrolet" and year == 2021`, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`year == 2021 and brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`model == nil`, `{"id":114905,"model":null,"brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`model != nil`, `{"id":114905,"model":null,"brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`model == nil`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`model != nil`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
	{`model == "\"hello world\";v=\"42\", "`, `{"brand":{"name":"Chevrolet"},"id":27502,"model":"\\\"hello world\\\";v=\\\"42\\\", ","year":2021}`, true, 0},
	{`brand.name == "Chevrolet" and limit(100)`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 100},
	{`brand.name != "Chevrolet" and limit(100)`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 100},
	{`brand.name.startsWith()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name.endsWith()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`brand.name.contains()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`datetime()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, false, 0},
	{`!brand.name.startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0},
	{`!brand.name.startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0},
}

func TestEval(t *testing.T) {
	for _, row := range data {
		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}
		// repr.Println(expr)
		limit, err := Precompute(expr)
		if err != nil {
			t.Fatal(err.Error())
		}
		assert.Equal(t, limit, row.limit)
		// repr.Println(expr)
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
