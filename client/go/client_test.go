package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	HOST string = "localhost"
	PORT string = "9099"
)

func TestLimit(t *testing.T) {
	err := Limit(HOST, PORT, 1000000)
	assert.Nil(t, err)
}

func TestInsert(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	c, err := NewConnection(HOST, PORT)
	assert.Nil(t, err)

	c.InsertMode()
	for index := 0; index < 15000; index++ {
		c.SendText(payload)
	}
}

func TestSingle(t *testing.T) {
	id := 42
	data, err := Single(HOST, PORT, id)
	assert.Nil(t, err)

	expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, id)
	assert.Equal(t, expected, string(data))
}

func TestValidate(t *testing.T) {
	err := Validate(HOST, PORT, `brand.name == "Chevrolet"`)
	assert.Nil(t, err)

	err = Validate(HOST, PORT, `=.=`)
	assert.EqualError(t, err, `1:1: unexpected token "="`)

	err = Validate(HOST, PORT, `request.path[3.14] == "hello"`)
	assert.EqualError(t, err, `1:14: unexpected token "3.14" (expected (<string> | <char> | <rawstring>) "]")`)
}
