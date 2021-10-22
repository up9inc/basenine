package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	f := newPartition()
	assert.NotNil(t, f)
	assert.FileExists(t, fmt.Sprintf("%s_%09d.%s", DB_FILE, cs.partitionIndex, DB_FILE_EXT))
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

	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	f := newPartition()
	assert.NotNil(t, f)

	for index := 0; index < 100; index++ {
		expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, index)

		insertData([]byte(payload))

		// Safely acces the offsets and partition references
		n, rf, err := getOffsetAndPartition(index)
		assert.Nil(t, err)

		rf.Seek(n, io.SeekStart)
		b, n, err := readRecord(rf, n)
		assert.Nil(t, err)
		assert.Greater(t, n, int64(0))
		assert.Equal(t, expected, string(b))

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
	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("/insert\n"))
	client.Write([]byte(`{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`))
	client.Write([]byte("\n"))

	time.Sleep(500 * time.Millisecond)

	index := 0
	expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, index)

	// Safely acces the offsets and partition references
	n, rf, err := getOffsetAndPartition(index)
	assert.Nil(t, err)

	rf.Seek(n, io.SeekStart)
	b, n, err := readRecord(rf, n)
	assert.Nil(t, err)
	assert.Greater(t, n, int64(0))
	assert.Equal(t, expected, string(b))

	rf.Close()

	client.Close()
	server.Close()

	removeDatabaseFiles()

	time.Sleep(500 * time.Millisecond)
}

func TestServerProtocolQueryMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	query := `brand.name == "Chevrolet"`

	cs = ConcurrentSlice{
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
		index := 0
		for {
			scanner := bufio.NewScanner(conn)

			for {
				ok := scanner.Scan()
				text := scanner.Text()

				expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, index)
				index++
				assert.Equal(t, expected, string(text))

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
	client.Write([]byte("/query\n"))

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

func TestServerProtocolSingleMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	id := 42

	cs = ConcurrentSlice{
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
				text := scanner.Text()

				expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, id)
				assert.Equal(t, expected, string(text))

				assert.True(t, ok)
				return
			}
		}
	}

	var wg sync.WaitGroup
	go readConnection(&wg, client)
	wg.Add(1)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("/single\n"))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", id)))

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
	{`request.path[3.14] == "hello"`, `1:14: unexpected token "3.14" (expected (<string> | <char> | <rawstring>) "]")`},
}

func TestServerProtocolValidateMode(t *testing.T) {
	for _, row := range validateModeData {
		cs = ConcurrentSlice{
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
					text := scanner.Text()

					assert.Equal(t, row.response, string(text))

					assert.True(t, ok)
					return
				}
			}
		}

		var wg sync.WaitGroup
		go readConnection(&wg, client)
		wg.Add(1)

		client.SetWriteDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte("/validate\n"))

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

	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	server, client := net.Pipe()
	go handleConnection(server)

	f := newPartition()
	assert.NotNil(t, f)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("/macro\n"))

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
				text := scanner.Text()

				expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, index)
				index++
				assert.Equal(t, expected, string(text))

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
	client.Write([]byte("/query\n"))

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

func TestServerProtocolLimitMode(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	limit := int64(1000000) // 1MB

	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	server, client := net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("/limit\n"))

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte(fmt.Sprintf("%d\n", limit)))

	client.Close()
	server.Close()

	server, client = net.Pipe()
	go handleConnection(server)

	client.SetWriteDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("/insert\n"))

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
