// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.
//
// This is the client library for Basenine database server.
//
package basenine

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Metadata info that's streamed after each record
type Metadata struct {
	Current            uint64 `json:"current"`
	Total              uint64 `json:"total"`
	NumberOfWritten    uint64 `json:"numberOfWritten"`
	LeftOff            string `json:"leftOff"`
	TruncatedTimestamp int64  `json:"truncatedTimestamp"`
	NoMoreData         bool   `json:"noMoreData"`
}

// Commands refers to TCP connection modes.
const (
	CMD_INSERT           string = "/insert"
	CMD_INSERTION_FILTER string = "/insert-filter"
	CMD_QUERY            string = "/query"
	CMD_SINGLE           string = "/single"
	CMD_FETCH            string = "/fetch"
	CMD_VALIDATE         string = "/validate"
	CMD_MACRO            string = "/macro"
	CMD_LIMIT            string = "/limit"
	CMD_METADATA         string = "/metadata"
	CMD_FLUSH            string = "/flush"
	CMD_RESET            string = "/reset"
)

// Closing indicators
const (
	CloseChannel    = "%close%"
	CloseConnection = "%quit%"
)

// Software name
const SoftwareName = "Basenine"

// Connection is the struct that holds the TCP connection reference.
type Connection struct {
	net.Conn
}

// NewConnection establishes a new connection with the server in the address host:port.
// Returns a Connection reference and an error.
func NewConnection(host string, port string) (connection *Connection, err error) {
	dest := host + ":" + port
	var conn net.Conn
	conn, err = net.Dial("tcp", dest)
	connection = &Connection{conn}
	return
}

// Send sends given []byte to the server which the connection is established to.
func (c *Connection) Send(data []byte) (err error) {
	err = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err != nil {
		return
	}
	_, err = c.Write(data)
	return
}

// SendText is the wrapper around Send method that allows user to send text directly.
func (c *Connection) SendText(text string) (err error) {
	err = c.Send([]byte(fmt.Sprintf("%s\n", text)))
	return
}

// InsertMode turns the connection's mode into INSERT mode
func (c *Connection) InsertMode() (err error) {
	err = c.SendText(CMD_INSERT)
	return
}

// Query is the method that user should use to stream the records from the database.
// It takes the filtering language (query) as the first parameter and
// a []byte channel which the records will be streamed into as the second parameter.
// Third parameter is the channel for streaming metadata, progress of the query.
func (c *Connection) Query(query string, fetch int, timeoutMs int, data chan []byte, meta chan []byte) (err error) {
	query = escapeLineFeed(query)

	var wg sync.WaitGroup
	go readConnection(&wg, c, data, meta)
	wg.Add(1)

	err = c.SendText(CMD_QUERY)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(query)
	if err != nil {
		c.Close()
	}

	err = c.SendText(fmt.Sprintf("%d", fetch))
	if err != nil {
		c.Close()
	}

	err = c.SendText(fmt.Sprintf("%d", timeoutMs))
	if err != nil {
		c.Close()
	}

	return
}

// Single returns a single record from the database server specified by the host:port pair
// and by given ID.
func Single(host string, port string, id string, query string) (data []byte, err error) {
	query = escapeLineFeed(query)

	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_SINGLE)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%s", id))
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(query)
	if err != nil {
		c.Close()
		return
	}

	data = <-ret
	c.Close()
	return
}

// Fetch returns limit number of records by querying on either positive(future) or negative(past) direction
// that starts from leftOff.
func Fetch(host string, port string, leftOff string, direction int, query string, limit int, timeout time.Duration) (data [][]byte, meta []byte, err error) {
	query = escapeLineFeed(query)

	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	dataChan := make(chan []byte)
	metaChan := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, dataChan, metaChan)
	wg.Add(1)

	err = c.SendText(CMD_FETCH)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(leftOff)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%d", direction))
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(query)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%d", limit))
	if err != nil {
		c.Close()
		return
	}

	afterCh := time.After(timeout)
	counter := 0
	var receivedMeta bool
	var receivedData bool
	for {
		select {
		case record := <-dataChan:
			data = append(data, record)
			counter++
			if counter >= limit {
				receivedData = true
				if receivedMeta {
					c.Close()
					return
				}
			}
		case meta = <-metaChan:
			receivedMeta = true
			if receivedData {
				c.Close()
				return
			}
		case <-afterCh:
			c.Close()
			return
		}
	}
}

// Validate validates the given query against syntax errors by passing the query
// to the database server at host:port
func Validate(host string, port string, query string) (err error) {
	query = escapeLineFeed(query)

	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_VALIDATE)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%s", query))
	if err != nil {
		c.Close()
		return
	}

	data := <-ret
	text := string(data)
	if text != "OK" {
		err = errors.New(text)
	}
	c.Close()
	return
}

// Macro defines a macro that expands into expanded in the database server
// at host:port. Same macro can be overwritten by a second Macro call.
func Macro(host string, port string, macro string, expanded string) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_MACRO)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%s~%s", macro, expanded))
	if err != nil {
		c.Close()
		return
	}

	data := <-ret
	text := string(data)
	if text != "OK" {
		err = errors.New(text)
	}
	c.Close()
	return
}

// InsertionFilter set an insertion filter given in the query argument in the database server
// at host:port.
func InsertionFilter(host string, port string, query string) (err error) {
	query = escapeLineFeed(query)

	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_INSERTION_FILTER)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%s", query))
	if err != nil {
		c.Close()
		return
	}

	data := <-ret
	text := string(data)
	if text != "OK" {
		err = errors.New(text)
	}
	c.Close()
	return
}

// Limit sets a hard-limit for the database size in bytes like 1000000 (1MB)
// Such that actual disk usage ranges between half of the limit (soft-limit) and
// the limit's itself (hard-limit).
func Limit(host string, port string, limit int64) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_LIMIT)
	if err != nil {
		c.Close()
		return
	}

	err = c.SendText(fmt.Sprintf("%d", limit))
	if err != nil {
		c.Close()
		return
	}

	data := <-ret
	text := string(data)
	if text != "OK" {
		err = errors.New(text)
	}
	c.Close()
	return
}

// Flush removes all the records in the database.
func Flush(host string, port string) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_FLUSH)
	if err != nil {
		c.Close()
		return
	}

	data := <-ret
	text := string(data)
	if text != "OK" {
		err = errors.New(text)
	}
	c.Close()
	return
}

// Reset removes all the records in the database
// and resets the core into its initial state.
func Reset(host string, port string) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan []byte)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret, nil)
	wg.Add(1)

	err = c.SendText(CMD_RESET)
	if err != nil {
		c.Close()
		return
	}

	data := <-ret
	text := string(data)
	if text != "OK" {
		err = errors.New(text)
	}
	c.Close()
	return
}

// readConnection is a Goroutine that recieves messages from the TCP connection
// and sends them to a []byte channel provided by the data parameter.
func readConnection(wg *sync.WaitGroup, c *Connection, data chan []byte, meta chan []byte) {
	defer wg.Done()
	for {
		scanner := bufio.NewScanner(c)
		// Prevent buffer overflows
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 209715200)

		for {
			ok := scanner.Scan()

			if !ok {
				// Reached EOF on server connection.
				return
			}

			bytes := scanner.Bytes()

			command := handleCommands(bytes, meta)
			switch command {
			case CMD_METADATA:
				b := make([]byte, len(bytes[len(CMD_METADATA)+1:]))
				copy(b, bytes[len(CMD_METADATA)+1:])

				meta <- b
				continue
			case CloseConnection:
				log.Printf("%s client: Server is leaving. Hanging up.\n", SoftwareName)
				break
			}

			b := make([]byte, len(bytes))
			copy(b, bytes)

			data <- b
		}
	}
}

// handleCommands is used by readConnection to make the server's orders
// in the client to take effect. Such that the server can hang up
// the connection.
func handleCommands(bytes []byte, meta chan []byte) string {
	r, err := regexp.Compile("^%.*%$")
	text := string(bytes)
	if err == nil {
		if strings.HasPrefix(text, CMD_METADATA) {
			return CMD_METADATA
		} else if r.MatchString(text) {
			return CloseConnection
		}
	}

	return ""
}

func escapeLineFeed(query string) string {
	return strings.Replace(query, "\n", " ", -1)
}
