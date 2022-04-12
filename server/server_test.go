package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	basenine "github.com/up9inc/basenine/server/lib"
	"github.com/up9inc/basenine/server/lib/connectors"
)

func TestServerConnCheck(t *testing.T) {
	server, client := net.Pipe()
	assert.Nil(t, basenine.ConnCheck(client))
	assert.Nil(t, basenine.ConnCheck(server))
	client.Close()
	assert.Equal(t, io.ErrClosedPipe, basenine.ConnCheck(client))
	server.Close()
	assert.Equal(t, io.ErrClosedPipe, basenine.ConnCheck(server))
}

func TestServerProtocolInsertMode(t *testing.T) {
	connector = connectors.NewNativeConnector(false)

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

	// TODO: Revive the commented the out section
	// index := 0
	// expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))

	// // Safely acces the offsets and partition references
	// n, rf, err := connector.GetOffsetAndPartition(index)
	// assert.Nil(t, err)

	// rf.Seek(n, io.SeekStart)
	// b, n, err := readRecord(rf, n)
	// assert.Nil(t, err)
	// assert.Greater(t, n, int64(0))
	// assert.JSONEq(t, expected, string(b))

	// rf.Close()

	client.Close()
	server.Close()

	connector.Reset()

	time.Sleep(500 * time.Millisecond)
}

func TestServerProtocolInsertionFilterMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	insertionFilter := `brand.name == "Chevrolet" and redact("year")`
	query := `brand.name == "Chevrolet"`

	connector = connectors.NewNativeConnector(false)

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
		connector.InsertData([]byte(payload))
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

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		connector.Reset()
	}
}

var testServerProtocolQueryModeData = []struct {
	query   string
	limit   int
	rlimit  int
	leftOff int
}{
	{`brand.name == "Chevrolet"`, 100, 100, 0},
	{`brand.name == "Chevrolet" and limit(10)`, 10, 100, 0},
	{`limit(10) and brand.name == "Chevrolet"`, 10, 100, 0},
	{`brand.name == "Chevrolet" and rlimit(10)`, 10, 10, 0},
	{`rlimit(10) and brand.name == "Chevrolet"`, 10, 10, 0},
	{`leftOff(60) and brand.name == "Chevrolet"`, 40, 100, 60},
}

func TestServerProtocolQueryMode(t *testing.T) {
	for _, row := range testServerProtocolQueryModeData {
		payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

		connector = connectors.NewNativeConnector(false)

		server, client := net.Pipe()
		go handleConnection(server)

		total := 100

		for index := 0; index < total; index++ {
			connector.InsertData([]byte(payload))
		}

		readConnection := func(wg *sync.WaitGroup, conn net.Conn) {
			defer wg.Done()
			index := total - row.rlimit + row.leftOff
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

		if waitTimeout(&wg, 1*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			connector.Reset()
		}
	}
}

func TestServerProtocolSingleMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	id := 42

	connector = connectors.NewNativeConnector(false)

	server, client := net.Pipe()
	go handleConnection(server)

	for index := 0; index < 100; index++ {
		connector.InsertData([]byte(payload))
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

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		connector.Reset()
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
		connector = connectors.NewNativeConnector(false)

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

		if waitTimeout(&wg, 1*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			connector.Reset()
		}
	}
}

func TestServerProtocolMacroMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	macro := `chevy~brand.name == "Chevrolet"`
	query := `chevy`

	connector = connectors.NewNativeConnector(false)

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
		connector.InsertData([]byte(payload))
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

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		connector.Reset()
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

		connector = connectors.NewNativeConnector(false)

		server, client := net.Pipe()
		go handleConnection(server)

		total := 100

		for index := 0; index < total; index++ {
			connector.InsertData([]byte(payload))
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

		if waitTimeout(&wg, 10*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			connector.Reset()
		}
	}
}

func TestServerProtocolLimitMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	limit := int64(1000000) // 1MB

	connector = connectors.NewNativeConnector(false)

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

	// TODO: Revive the commented the out section
	// var lastFile, secondLastFile *os.File

	// cs.RLock()
	// lastFile = cs.partitions[cs.partitionIndex]
	// secondLastFile = cs.partitions[cs.partitionIndex-1]
	// cs.RUnlock()

	// lastFileInfo, err := lastFile.Stat()
	// assert.Nil(t, err)
	// secondLastFileInfo, err := secondLastFile.Stat()
	// assert.Nil(t, err)

	// assert.Less(t, lastFileInfo.Size(), limit)
	// assert.Less(t, secondLastFileInfo.Size(), limit)

	client.Close()
	server.Close()

	connector.Reset()
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
