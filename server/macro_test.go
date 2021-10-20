package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMacro(t *testing.T) {
	query := `
http or !amqp and request.method == "GET" and request.headers["http"] == "amqp"
	`
	expected := `
(proto.name == "http") or !(proto.name == "amqp") and request.method == "GET" and request.headers["http"] == "amqp"
	`

	addMacro("http", "proto.name == \"http\"")
	addMacro("amqp", "proto.name == \"amqp\"")

	newQuery, err := expandMacros(query)
	assert.Nil(t, err)
	assert.Equal(t, expected, newQuery)
}
