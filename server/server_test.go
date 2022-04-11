package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	basenine "github.com/up9inc/basenine/server/lib"
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

func TestServerNewPartition(t *testing.T) {
	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
	}

	f := newPartition()
	assert.NotNil(t, f)
	assert.FileExists(t, fmt.Sprintf("%s_%09d.%s", DB_FILE, cs.partitionIndex, DB_FILE_EXT))
}

func TestServerDumpRestoreCore(t *testing.T) {
	err := dumpCore(false, false)
	assert.Nil(t, err)

	err = restoreCore()
	assert.Nil(t, err)

	removeDatabaseFiles()
}

func TestServerCheckError(t *testing.T) {
	assert.Panics(t, assert.PanicTestFunc(func() {
		check(errors.New("something"))
	}))

	assert.NotPanics(t, assert.PanicTestFunc(func() {
		check(nil)
	}))
}

func TestServerInsertAndReadData(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
	}

	f := newPartition()
	assert.NotNil(t, f)

	for index := 0; index < 100; index++ {
		expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))

		insertData([]byte(payload))

		// Safely acces the offsets and partition references
		n, rf, err := getOffsetAndPartition(index)
		assert.Nil(t, err)

		rf.Seek(n, io.SeekStart)
		b, n, err := readRecord(rf, n)
		assert.Nil(t, err)
		assert.Greater(t, n, int64(0))
		assert.JSONEq(t, expected, string(b))

		rf.Close()
	}

	removeDatabaseFiles()
}

func TestServerConnCheck(t *testing.T) {
	server, client := net.Pipe()
	assert.Nil(t, connCheck(client))
	assert.Nil(t, connCheck(server))
	client.Close()
	assert.Equal(t, io.ErrClosedPipe, connCheck(client))
	server.Close()
	assert.Equal(t, io.ErrClosedPipe, connCheck(server))
}

func TestServerProtocolInsertMode(t *testing.T) {
	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
	}

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_INSERT)))
	client.Write([]byte(`{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`))
	client.Write([]byte("\n"))

	// Case for non-JSON payload
	client.Write([]byte(`hello world`))
	client.Write([]byte("\n"))

	time.Sleep(500 * time.Millisecond)

	index := 0
	expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))

	// Safely acces the offsets and partition references
	n, rf, err := getOffsetAndPartition(index)
	assert.Nil(t, err)

	rf.Seek(n, io.SeekStart)
	b, n, err := readRecord(rf, n)
	assert.Nil(t, err)
	assert.Greater(t, n, int64(0))
	assert.JSONEq(t, expected, string(b))

	rf.Close()

	client.Close()
	server.Close()

	removeDatabaseFiles()

	time.Sleep(500 * time.Millisecond)
}

func TestServerProtocolInsertionFilterMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	insertionFilter := `brand.name == "Chevrolet" and redact("year")`
	query := `brand.name == "Chevrolet"`

	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
		macros:         make(map[string]string),
	}

	server, client := net.Pipe()
	go handleConnection(server)

	f := newPartition()
	assert.NotNil(t, f)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_INSERTION_FILTER)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", insertionFilter)))

	client.Close()
	server.Close()

	time.Sleep(500 * time.Millisecond)

	server, client = net.Pipe()
	go handleConnection(server)

	for index := 0; index < 100; index++ {
		insertData([]byte(payload))
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
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_QUERY)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", query)))

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		removeDatabaseFiles()
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

		cs.Lock()
		cs = ConcurrentSliceV0{
			partitionIndex: -1,
		}

		server, client := net.Pipe()
		go handleConnection(server)

		f := newPartition()
		assert.NotNil(t, f)

		total := 100

		for index := 0; index < total; index++ {
			insertData([]byte(payload))
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
		client.Write([]byte(fmt.Sprintf("%s\n", CMD_QUERY)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", row.query)))

		if waitTimeout(&wg, 1*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			removeDatabaseFiles()
		}
	}
}

func TestServerProtocolSingleMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	id := 42

	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
	}

	server, client := net.Pipe()
	go handleConnection(server)

	f := newPartition()
	assert.NotNil(t, f)

	for index := 0; index < 100; index++ {
		insertData([]byte(payload))
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
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_SINGLE)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", id)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("\n"))

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		removeDatabaseFiles()
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
		cs.Lock()
		cs = ConcurrentSliceV0{
			partitionIndex: -1,
		}

		server, client := net.Pipe()
		go handleConnection(server)

		f := newPartition()
		assert.NotNil(t, f)

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
		client.Write([]byte(fmt.Sprintf("%s\n", CMD_VALIDATE)))

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(fmt.Sprintf("%s\n", row.query)))

		if waitTimeout(&wg, 1*time.Second) {
			t.Fatal("Timed out waiting for wait group")
		} else {
			client.Close()
			server.Close()

			removeDatabaseFiles()
		}
	}
}

func TestServerProtocolMacroMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	macro := `chevy~brand.name == "Chevrolet"`
	query := `chevy`

	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
		macros:         make(map[string]string),
	}

	server, client := net.Pipe()
	go handleConnection(server)

	f := newPartition()
	assert.NotNil(t, f)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_MACRO)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", macro)))

	client.Close()
	server.Close()

	server, client = net.Pipe()
	go handleConnection(server)

	for index := 0; index < 100; index++ {
		insertData([]byte(payload))
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
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_QUERY)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", query)))

	if waitTimeout(&wg, 1*time.Second) {
		t.Fatal("Timed out waiting for wait group")
	} else {
		client.Close()
		server.Close()

		removeDatabaseFiles()
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

		cs.Lock()
		cs = ConcurrentSliceV0{
			partitionIndex: -1,
		}

		server, client := net.Pipe()
		go handleConnection(server)

		f := newPartition()
		assert.NotNil(t, f)

		total := 100

		for index := 0; index < total; index++ {
			insertData([]byte(payload))
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
		client.Write([]byte(fmt.Sprintf("%s\n", CMD_FETCH)))

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

			removeDatabaseFiles()
		}
	}
}

func TestServerProtocolLimitMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	limit := int64(1000000) // 1MB

	cs.Lock()
	cs = ConcurrentSliceV0{
		partitionIndex: -1,
	}

	f := newPartition()
	assert.NotNil(t, f)

	// Trigger partitioning check for every second.
	ticker := time.NewTicker(1 * time.Second)
	go periodicPartitioner(ticker)

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_LIMIT)))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", limit)))

	client.Close()
	server.Close()

	server, client = net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_INSERT)))

	for index := 0; index < 15000; index++ {
		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(payload))
		client.Write([]byte("\n"))
		time.Sleep(500 * time.Microsecond)
	}

	var lastFile, secondLastFile *os.File

	cs.RLock()
	lastFile = cs.partitions[cs.partitionIndex]
	secondLastFile = cs.partitions[cs.partitionIndex-1]
	cs.RUnlock()

	lastFileInfo, err := lastFile.Stat()
	assert.Nil(t, err)
	secondLastFileInfo, err := secondLastFile.Stat()
	assert.Nil(t, err)

	assert.Less(t, lastFileInfo.Size(), limit)
	assert.Less(t, secondLastFileInfo.Size(), limit)

	client.Close()
	server.Close()

	removeDatabaseFiles()
}

func TestServerProtocolFlushMode(t *testing.T) {
	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_FLUSH)))

	client.Close()
	server.Close()
}

func TestServerProtocolResetMode(t *testing.T) {
	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%s\n", CMD_RESET)))

	client.Close()
	server.Close()
}

func TestServerFlush(t *testing.T) {
	insertionFilter := "model"
	insertionFilterExpr, _, err := prepareQuery(insertionFilter)
	assert.Nil(t, err)
	macros := map[string]string{"foo": "bar"}
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	cs.Lock()
	cs = ConcurrentSliceV0{
		version:             VERSION,
		partitionIndex:      -1,
		macros:              macros,
		insertionFilter:     insertionFilter,
		insertionFilterExpr: insertionFilterExpr,
	}

	f := newPartition()
	assert.NotNil(t, f)

	insertData([]byte(payload))

	flush()

	cs.RLock()
	assert.Equal(t, cs.version, VERSION)
	assert.Empty(t, cs.lastOffset)
	assert.Empty(t, cs.partitionRefs)
	assert.Empty(t, cs.offsets)
	assert.Len(t, cs.partitions, 1)
	assert.Empty(t, cs.partitionIndex)
	assert.Empty(t, cs.partitionSizeLimit)
	assert.Empty(t, cs.truncatedTimestamp)
	assert.Empty(t, cs.removedOffsetsCounter)
	assert.Equal(t, cs.macros, macros)
	assert.Equal(t, cs.insertionFilter, insertionFilter)
	assert.Equal(t, cs.insertionFilterExpr, insertionFilterExpr)
	cs.RUnlock()
}

func TestServerReset(t *testing.T) {
	insertionFilter := "model"
	insertionFilterExpr, _, err := prepareQuery(insertionFilter)
	assert.Nil(t, err)
	macros := map[string]string{"foo": "bar"}
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	cs.Lock()
	cs = ConcurrentSliceV0{
		version:             VERSION,
		partitionIndex:      -1,
		macros:              macros,
		insertionFilter:     insertionFilter,
		insertionFilterExpr: insertionFilterExpr,
	}

	f := newPartition()
	assert.NotNil(t, f)

	insertData([]byte(payload))

	reset()

	cs.RLock()
	assert.Equal(t, cs.version, VERSION)
	assert.Empty(t, cs.lastOffset)
	assert.Empty(t, cs.partitionRefs)
	assert.Empty(t, cs.offsets)
	assert.Len(t, cs.partitions, 1)
	assert.Empty(t, cs.partitionIndex)
	assert.Empty(t, cs.partitionSizeLimit)
	assert.Empty(t, cs.truncatedTimestamp)
	assert.Empty(t, cs.removedOffsetsCounter)
	assert.Empty(t, cs.macros)
	assert.Empty(t, cs.insertionFilter)
	assert.Empty(t, cs.insertionFilterExpr)
	cs.RUnlock()
}

// handleCommands is used by readConnection to make the server's orders
// in the client to take effect. Such that the server can hang up
// the connection.
func handleCommands(bytes []byte) bool {
	r, err := regexp.Compile("^%.*%$")
	text := string(bytes)
	if err == nil {
		if strings.HasPrefix(text, CMD_METADATA) {
			return true
		} else if r.MatchString(text) {

			switch {
			case text == CloseConnection:
				log.Println("Server is leaving. Hanging up.")
			}

			return true
		}
	}

	return false
}
