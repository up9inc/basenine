package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMacro(t *testing.T) {
	query := `
http or !amqp and request.method == "GET" and request.headers["http"] == "x-amqp-y"

http or http2 or !amqp and request.method == "GET" and request.headers["http"] == "x-amqp-y"
	`
	expected := `
(proto.name == "http") or !(proto.name == "amqp") and request.method == "GET" and request.headers["http"] == "x-amqp-y"

(proto.name == "http") or (proto.name == "http2") or !(proto.name == "amqp") and request.method == "GET" and request.headers["http"] == "x-amqp-y"
	`

	addMacro("http", "proto.name == \"http\"")
	addMacro("http2", "proto.name == \"http2\"")
	addMacro("amqp", "proto.name == \"amqp\"")

	newQuery, err := expandMacros(query)
	assert.Nil(t, err)
	assert.Equal(t, expected, newQuery)
}
