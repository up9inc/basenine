package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	HOST string = "localhost"
	PORT string = "9099"
)

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func TestLimit(t *testing.T) {
	err := Limit(HOST, PORT, 1000000)
	assert.Nil(t, err)
}

func TestMacro(t *testing.T) {
	err := Macro(HOST, PORT, "chevy", `brand.name == "Chevrolet"`)
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

func TestQuery(t *testing.T) {
	c, err := NewConnection(HOST, PORT)
	assert.Nil(t, err)

	data := make(chan []byte)

	handleDataChannel := func(wg *sync.WaitGroup, c *Connection, data chan []byte) {
		defer wg.Done()
		index := 0
		for {
			bytes := <-data
			text := string(bytes)

			expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, index)
			index++
			assert.Equal(t, expected, text)

			if index > 14000 {
				c.Close()
				return
			}
		}
	}

	var wg sync.WaitGroup
	go handleDataChannel(&wg, c, data)
	wg.Add(1)

	c.Query(`chevy`, data)
	assert.Nil(t, err)

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	}
}
