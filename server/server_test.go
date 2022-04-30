package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	basenine "github.com/up9inc/basenine/server/lib"
	"github.com/up9inc/basenine/server/lib/storages"
)

func TestServerProtocolInsertMode(t *testing.T) {
	storage = storages.NewNativeStorage(false)

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_INSERT)))
	client.Write([]byte(`{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`))
	client.Write([]byte("\n"))

	// Case for non-JSON payload
	client.Write([]byte(`hello world`))
	client.Write([]byte("\n"))

	time.Sleep(500 * time.Millisecond)

	client.Close()
	server.Close()

	storage.Reset()

	time.Sleep(500 * time.Millisecond)
}

func TestServerProtocolInsertionFilterMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	insertionFilter := `brand.name == "Chevrolet" and redact("year")`
	query := `brand.name == "Chevrolet"`

	storage = storages.NewNativeStorage(false)

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_INSERTION_FILTER)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", insertionFilter)))

	client.Close()
	server.Close()

	time.Sleep(500 * time.Millisecond)

	server, client = net.Pipe()
	go handleConnection(server)

	for index := 0; index < 100; index++ {
		storage.InsertData([]byte(payload))
	}

	readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
		defer wg.Done()
		index := 0
		for {
			scanner := bufio.NewScanner(conn)

			for {
				ok := scanner.Scan()
				bytes := scanner.Bytes()

				command := handleCommands(bytes)
				if command {
					break
				}

				expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":"%s"}`, basenine.IndexToID(index), basenine.REDACTED)
				index++
				assert.JSONEq(t, expected, string(bytes))

				if index > 99 {
					return
				}

				assert.True(t, ok)
			}
		}
	}

	var wg sync.WaitGroup
	go readConnection(&wg, client)
	wg.Add(1)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_QUERY)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", query)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", 0)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", 0)))

	if basenine.WaitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		storage.Reset()
	}
}

var testServerProtocolQueryModeData = []struct {
	query   string
	limit   int
	leftOff int
}{
	{`brand.name == "Chevrolet"`, 100, 0},
	{`brand.name == "Chevrolet" and limit(10)`, 10, 0},
	{`limit(10) and brand.name == "Chevrolet"`, 10, 0},
	{`leftOff(60) and brand.name == "Chevrolet"`, 40, 60},
}

func TestServerProtocolQueryMode(t *testing.T) {
	for _, row := range testServerProtocolQueryModeData {
		payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

		storage = storages.NewNativeStorage(false)

		server, client := net.Pipe()
		go handleConnection(server)

		total := 100

		for index := 0; index < total; index++ {
			storage.InsertData([]byte(payload))
		}

		readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
			defer wg.Done()
			index := row.leftOff
			if row.leftOff != 0 {
				index++
			}
			for {
				scanner := bufio.NewScanner(conn)

				for {
					ok := scanner.Scan()
					bytes := scanner.Bytes()

					command := handleCommands(bytes)
					if command {
						break
					}

					expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))
					index++
					assert.JSONEq(t, expected, string(bytes))

					if index > (row.limit - 1) {
						return
					}

					assert.True(t, ok)
				}
			}
		}

		var wg sync.WaitGroup
		go readConnection(&wg, client)
		wg.Add(1)

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_QUERY)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", row.query)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%d\n", 0)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%d\n", 0)))

		if basenine.WaitTimeout(&wg, 1*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			storage.Reset()
		}
	}
}

func TestServerProtocolSingleMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	id := 42

	storage = storages.NewNativeStorage(false)

	server, client := net.Pipe()
	go handleConnection(server)

	for index := 0; index < 100; index++ {
		storage.InsertData([]byte(payload))
	}

	readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
		defer wg.Done()
		for {
			scanner := bufio.NewScanner(conn)

			for {
				ok := scanner.Scan()
				bytes := scanner.Bytes()

				command := handleCommands(bytes)
				if command {
					break
				}

				expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(id))
				assert.JSONEq(t, expected, string(bytes))

				assert.True(t, ok)
				return
			}
		}
	}

	var wg sync.WaitGroup
	go readConnection(&wg, client)
	wg.Add(1)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_SINGLE)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", id)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("\n"))

	if basenine.WaitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		storage.Reset()
	}
}

var validateModeData = []struct {
	query    string
	response string
}{
	{`brand.name == "Chevrolet"`, `OK`},
	{`=.=`, `1:1: unexpected token "="`},
	{`request.path[3.14] == "hello"`, `1:14: unexpected token "3.14" (expected (<string> | <char> | <rawstring> | "*") "]")`},
}

func TestServerProtocolValidateMode(t *testing.T) {
	for _, row := range validateModeData {
		storage = storages.NewNativeStorage(false)

		server, client := net.Pipe()
		go handleConnection(server)

		readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
			defer wg.Done()
			for {
				scanner := bufio.NewScanner(conn)

				for {
					ok := scanner.Scan()
					bytes := scanner.Bytes()

					command := handleCommands(bytes)
					if command {
						break
					}

					assert.Equal(t, row.response, string(bytes))

					assert.True(t, ok)
					return
				}
			}
		}

		var wg sync.WaitGroup
		go readConnection(&wg, client)
		wg.Add(1)

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_VALIDATE)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", row.query)))

		if basenine.WaitTimeout(&wg, 1*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			storage.Reset()
		}
	}
}

func TestServerProtocolMacroMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	macro := `chevy~brand.name == "Chevrolet"`
	query := `chevy`

	storage = storages.NewNativeStorage(false)

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_MACRO)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", macro)))

	client.Close()
	server.Close()

	server, client = net.Pipe()
	go handleConnection(server)

	for index := 0; index < 100; index++ {
		storage.InsertData([]byte(payload))
	}

	readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
		defer wg.Done()
		index := 0
		for {
			scanner := bufio.NewScanner(conn)

			for {
				ok := scanner.Scan()
				bytes := scanner.Bytes()

				command := handleCommands(bytes)
				if command {
					break
				}

				expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))
				index++
				assert.JSONEq(t, expected, string(bytes))

				if index > 99 {
					return
				}

				assert.True(t, ok)
			}
		}
	}

	var wg sync.WaitGroup
	go readConnection(&wg, client)
	wg.Add(1)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_QUERY)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", query)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", 0)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", 0)))

	if basenine.WaitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		storage.Reset()
	}
}

var testServerProtocolFetchModeData = []struct {
	leftOff   int
	direction int
	query     string
	limit     int
	expected  int
}{
	{0, 1, `brand.name == "Chevrolet"`, 5, 5},
	{13, 1, `brand.name == "Chevrolet"`, 5, 5},
	{13, 1, `brand.name == "Chevrolet"`, 200, 87},
	{93, 1, `brand.name == "Chevrolet"`, 20, 7},
	{99, -1, `brand.name == "Chevrolet"`, 5, 5},
	{13, -1, `brand.name == "Chevrolet"`, 5, 5},
	{13, -1, `brand.name == "Chevrolet"`, 200, 13},
	{93, -1, `brand.name == "Chevrolet"`, 20, 20},
}

func TestServerProtocolFetchMode(t *testing.T) {
	for _, row := range testServerProtocolFetchModeData {
		payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

		storage = storages.NewNativeStorage(false)

		server, client := net.Pipe()
		go handleConnection(server)

		total := 100

		for index := 0; index < total; index++ {
			storage.InsertData([]byte(payload))
		}

		readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
			defer wg.Done()
			index := row.leftOff
			if row.direction < 0 {
				index--
			}
			counter := 0
			for {
				scanner := bufio.NewScanner(conn)

				for {
					ok := scanner.Scan()
					bytes := scanner.Bytes()

					command := handleCommands(bytes)
					if command {
						break
					}

					if row.direction < 0 {
						index--
					} else {
						index++
					}

					if index >= total || index < 0 {
						return
					}

					expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))
					assert.JSONEq(t, expected, string(bytes))

					counter++

					if counter >= row.expected {
						return
					}

					assert.True(t, ok)
				}
			}
		}

		var wg sync.WaitGroup
		go readConnection(&wg, client)
		wg.Add(1)

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_FETCH)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%d\n", row.leftOff)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%d\n", row.direction)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", row.query)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%d\n", row.limit)))

		if basenine.WaitTimeout(&wg, 10*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			storage.Reset()
		}
	}
}

func TestServerProtocolLimitMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	limit := int64(1000000) // 1MB

	storage = storages.NewNativeStorage(false)

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_LIMIT)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", limit)))

	client.Close()
	server.Close()

	server, client = net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_INSERT)))

	for index := 0; index < 15000; index++ {
		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(payload))
		client.Write([]byte("\n"))
		time.Sleep(500 * time.Microsecond)
	}

	client.Close()
	server.Close()

	storage.Reset()
}

func TestServerProtocolFlushMode(t *testing.T) {
	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_FLUSH)))

	client.Close()
	server.Close()
}

func TestServerProtocolResetMode(t *testing.T) {
	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", basenine.CMD_RESET)))

	client.Close()
	server.Close()
}

// handleCommands is used by readConnection to make the server's orders
// in the client to take effect. Such that the server can hang up
// the connection.
func handleCommands(bytes []byte) bool {
	r, err := regexp.Compile("^%.*%$")
	text := string(bytes)
	if err == nil {
		if strings.HasPrefix(text, basenine.CMD_METADATA) {
			return true
		} else if r.MatchString(text) {

			switch {
			case text == basenine.CloseConnection:
				log.Println("Server is leaving. Hanging up.")
			}

			return true
		}
	}

	return false
}
