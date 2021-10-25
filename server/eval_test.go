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
	{`42 > 41`, `{}`, true},
	{`42 >= 42`, `{}`, true},
	{`41 >= 42`, `{}`, false},
	{`13 < 42`, `{}`, true},
	{`42 < 13`, `{}`, false},
	{`!true`, `{}`, false},
	{`-300 < 42`, `{}`, true},
	{`true and !(5 == a)`, `{"a": 4}`, true},
	{`true and !(5 == a)`, `{"a": 5}`, false},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"hello"},"x":{"y":3.15}}`, true},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"hello"},"x":{"y":3.13}}`, false},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"mello"},"x":{"y":3.15}}`, false},
	{`brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand.name != "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand.game == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand.name == r"Chev.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand.name != r"Chev.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand.name == r"Bug.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand.name != r"Bug.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`request.path[1] == "v1"`, `{"request":{"path":["api","v1","example"]}}`, true},
	{`request.path[1] != "v1"`, `{"request":{"path":["api","v1","example"]}}`, false},
	{`request.headers["a"] == "b"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, true},
	{`request.headers["a"] != "b"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, false},
	{`request.headers["a"] == "d"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, false},
	{`request.headers["e"].x == "y"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, true},
	{`request.headers["e"].x == "z"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, false},
	{`request.headers["e"].x != "y"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, false},
	{`request.headers["e"].x != "z"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, true},
	{`brand.name.startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand.name.startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand.name.endsWith("let")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand.name.endsWith("le")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand.name.contains("ro")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand.name.contains("hello")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand["name"].startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand["name"].startsWithx("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`brand["name"].startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false},
	{`timestamp > datetime("10/19/2021, 6:29:02 PM")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, true},
	{`timestamp > datetime("10/19/2021, 7:29:02 PM")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, false},
	{`request.headers["a"] == "b" and request.path[1] == "v1"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, true},
	{`year == salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`, true},
	{`year == salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, false},
	{`year != salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, true},
	{`year != salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`, false},
	{`year > salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, true},
	{`year > salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2022}`, false},
	{`brand.name == "Chevrolet" and year == 2021`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true},
	{`brand.name == "Chevrolet" and year == 2021`, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`, false},
	{`year == 2021 and brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`, false},
}

func TestEval(t *testing.T) {
	for _, row := range data {
		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}
		// repr.Println(expr)
		err = Precompute(expr)
		if err != nil {
			t.Fatal(err.Error())
		}
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
