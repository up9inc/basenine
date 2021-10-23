package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"sync"
	"time"
)

type Connection struct {
	net.Conn
}

func NewConnection(host string, port string) (connection *Connection, err error) {
	dest := host + ":" + port
	var conn net.Conn
	conn, err = net.Dial("tcp", dest)
	connection = &Connection{conn}
	return
}

func (c *Connection) Send(data []byte) {
	c.SetWriteDeadline(time.Now().Add(1 * time.Second))
	c.Write(data)
}

func (c *Connection) SendText(text string) {
	c.Send([]byte(fmt.Sprintf("%s\n", text)))
}

func (c *Connection) InsertMode() {
	c.SendText("/insert")
}

func (c *Connection) Query(query string, data chan string) {
	var wg sync.WaitGroup
	go readConnection(&wg, c, data)
	wg.Add(1)

	c.SendText("/query")
	c.SendText(fmt.Sprintf("%s", query))

	wg.Wait()
}

func Single(host string, port string, id int) (data string, err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan string)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret)
	wg.Add(1)

	c.SendText("/single")
	c.SendText(fmt.Sprintf("%d", id))

	data = <-ret
	return
}

func Validate(host string, port string, query string) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan string)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret)
	wg.Add(1)

	c.SendText("/validate")
	c.SendText(fmt.Sprintf("%s", query))

	data := <-ret
	if data != "OK" {
		err = errors.New(data)
	}
	return
}

func Macro(host string, port string, macro string, expanded string) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan string)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret)
	wg.Add(1)

	c.SendText("/macro")
	c.SendText(fmt.Sprintf("%s~%s", macro, expanded))

	data := <-ret
	if data != "OK" {
		err = errors.New(data)
	}
	return
}

func Limit(host string, port string, limit int64) (err error) {
	var c *Connection
	c, err = NewConnection(host, port)
	if err != nil {
		return
	}

	ret := make(chan string)

	var wg sync.WaitGroup
	go readConnection(&wg, c, ret)
	wg.Add(1)

	c.SendText("/limit")
	c.SendText(fmt.Sprintf("%d", limit))

	data := <-ret
	if data != "OK" {
		err = errors.New(data)
	}
	return
}

func readConnection(wg *sync.WaitGroup, c *Connection, ret chan string) {
	defer wg.Done()
	for {
		scanner := bufio.NewScanner(c)

		for {
			ok := scanner.Scan()
			text := scanner.Text()

			command := handleCommands(text)
			if command {
				break
			}

			ret <- text

			if !ok {
				log.Println("Reached EOF on server connection.")
				break
			}
		}
	}
}

func handleCommands(text string) bool {
	r, err := regexp.Compile("^%.*%$")
	if err == nil {
		if r.MatchString(text) {

			switch {
			case text == "%quit%":
				log.Println("\b\bServer is leaving. Hanging up.")
			}

			return true
		}
	}

	return false
}