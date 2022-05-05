package storages

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	basenine "github.com/up9inc/basenine/server/lib"
)

func TestNativeStorageNewPartition(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	f := storage.newPartition()
	assert.NotNil(t, f)
	assert.FileExists(t, fmt.Sprintf("%s_%09d.%s", NATIVE_STORAGE_DB_FILE, storage.partitionIndex, NATIVE_STORAGE_DB_FILE_EXT))
}

func TestNativeStorageDumpRestoreCore(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	err := storage.DumpCore(false, false)
	assert.Nil(t, err)

	err = storage.RestoreCore()
	assert.Nil(t, err)

	storage.removeDatabaseFiles()
}

func TestNativeStorageInsertAndReadData(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	storage := NewNativeStorage(false).(*nativeStorage)

	for index := 0; index < 100; index++ {
		expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))

		storage.InsertData([]byte(payload))

		// Safely acces the offsets and partition references
		n, rf, err := storage.getOffsetAndPartition(uint64(index))
		assert.Nil(t, err)

		rf.Seek(n, io.SeekStart)
		b, n, err := storage.readRecord(rf, n)
		assert.Nil(t, err)
		assert.Greater(t, n, int64(0))
		assert.JSONEq(t, expected, string(b))

		rf.Close()
	}

	storage.Reset()
}

func TestNativeStorageMacros(t *testing.T) {
	key := `chevy`
	value := `brand.name == "Chevrolet"`
	macro := fmt.Sprintf(`%s~%s`, key, value)

	storage := NewNativeStorage(false).(*nativeStorage)

	server, client := net.Pipe()
	go func() {
		storage.ApplyMacro(server, []byte(macro))
		macros, err := storage.GetMacros()
		assert.Nil(t, err)
		assert.Len(t, macros, 1)
		assert.Equal(t, macros[key], fmt.Sprintf("(%s)", value))
		server.Close()
	}()

	time.Sleep(500 * time.Millisecond)

	client.Close()

	storage.removeDatabaseFiles()
}

func TestNativeStorageStreamRecords(t *testing.T) {
	query := ""
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	storage := NewNativeStorage(false).(*nativeStorage)

	for index := 0; index < 100; index++ {
		storage.InsertData([]byte(payload))
	}

	server, client := net.Pipe()
	go func() {
		storage.StreamRecords(server, "", query)
		server.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	client.Close()

	storage.Reset()
}

func TestNativeStorageRetrieveSingle(t *testing.T) {
	index := 42
	query := ""
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))

	storage := NewNativeStorage(false).(*nativeStorage)

	for i := 0; i < 100; i++ {
		storage.InsertData([]byte(payload))
	}

	server, client := net.Pipe()
	go func() {
		storage.RetrieveSingle(server, basenine.IndexToID(index), query)
		server.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	bytes, err := ioutil.ReadAll(client)
	assert.Nil(t, err)
	assert.JSONEq(t, expected, string(bytes))

	client.Close()

	storage.Reset()
}

func TestNativeStorageFetch(t *testing.T) {
	query := ""
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	storage := NewNativeStorage(false).(*nativeStorage)

	for index := 0; index < 100; index++ {
		storage.InsertData([]byte(payload))
	}

	server, client := net.Pipe()
	go func() {
		storage.Fetch(server, basenine.IndexToID(42), "-1", query, "20")
		server.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	bytes, err := ioutil.ReadAll(client)
	assert.Nil(t, err)
	assert.Len(t, strings.Split(string(bytes), "\n"), 41)

	client.Close()

	storage.Reset()
}

func TestNativeStorageSetLimit(t *testing.T) {
	limit := 1000000 // 1MB

	storage := NewNativeStorage(false).(*nativeStorage)

	server, client := net.Pipe()
	go func() {
		storage.SetLimit(server, []byte(fmt.Sprintf("%d", limit)))
		server.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	bytes, err := ioutil.ReadAll(client)
	assert.Nil(t, err)
	assert.Equal(t, "OK\n", string(bytes))

	client.Close()

	storage.Lock()
	assert.Equal(t, int64(limit/2), storage.partitionSizeLimit)
	storage.Unlock()
}

func TestNativeStorageSetInsertionFilter(t *testing.T) {
	insertionFilter := `brand.name == "Chevrolet" and redact("year")`

	storage := NewNativeStorage(false).(*nativeStorage)

	server, client := net.Pipe()
	go func() {
		storage.SetInsertionFilter(server, []byte(insertionFilter))
		server.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	bytes, err := ioutil.ReadAll(client)
	assert.Nil(t, err)
	assert.Equal(t, "OK\n", string(bytes))

	client.Close()

	storage.Lock()
	assert.Equal(t, insertionFilter, storage.insertionFilter)
	storage.Unlock()
}

var validateQueryData = []struct {
	query    string
	response string
}{
	{`brand.name == "Chevrolet"`, `OK`},
	{`=.=`, `1:1: unexpected token "="`},
	{`request.path[3.14] == "hello"`, `1:14: unexpected token "3.14" (expected (<string> | <char> | <rawstring> | "*") "]")`},
}

func TestNativeStorageValidateQuery(t *testing.T) {
	for _, row := range validateQueryData {
		storage := NewNativeStorage(false).(*nativeStorage)

		server, client := net.Pipe()
		go func() {
			storage.ValidateQuery(server, row.query)
			server.Close()
		}()

		time.Sleep(100 * time.Millisecond)

		bytes, err := ioutil.ReadAll(client)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("%s\n", row.response), string(bytes))

		client.Close()
	}
}

func TestNativeStorageSetPartitionSizeLimit(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`
	limit := 1000000 // 1MB

	storage := NewNativeStorage(false).(*nativeStorage)

	storage.setPartitionSizeLimit(limit)

	for index := 0; index < 15000; index++ {
		storage.InsertData([]byte(payload))
		time.Sleep(500 * time.Microsecond)
	}

	var lastFile, secondLastFile *os.File

	storage.RLock()
	lastFile = storage.partitions[storage.partitionIndex]
	secondLastFile = storage.partitions[storage.partitionIndex-1]
	storage.RUnlock()

	lastFileInfo, err := lastFile.Stat()
	assert.Nil(t, err)
	secondLastFileInfo, err := secondLastFile.Stat()
	assert.Nil(t, err)

	assert.Less(t, lastFileInfo.Size(), int64(limit))
	assert.Less(t, secondLastFileInfo.Size(), int64(limit))

	storage.Reset()
}

func TestNativeStorageFlush(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	insertionFilter := "model"
	macros := map[string]string{"foo": "bar"}
	insertionFilterExpr, _, err := storage.PrepareQuery(insertionFilter, macros)
	assert.Nil(t, err)
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	storage.Lock()
	storage.version = basenine.VERSION
	storage.partitionIndex = -1
	storage.macros = macros
	storage.insertionFilter = insertionFilter
	storage.insertionFilterExpr = insertionFilterExpr
	storage.Unlock()

	storage.InsertData([]byte(payload))

	storage.Flush()

	storage.RLock()
	assert.Equal(t, storage.version, basenine.VERSION)
	assert.Empty(t, storage.lastOffset)
	assert.Empty(t, storage.partitionRefs)
	assert.Empty(t, storage.offsets)
	assert.Len(t, storage.partitions, 1)
	assert.Empty(t, storage.partitionIndex)
	assert.Empty(t, storage.partitionSizeLimit)
	assert.Empty(t, storage.truncatedTimestamp)
	assert.Empty(t, storage.removedOffsetsCounter)
	assert.Equal(t, storage.macros, macros)
	assert.Equal(t, storage.insertionFilter, insertionFilter)
	assert.Equal(t, storage.insertionFilterExpr, insertionFilterExpr)
	storage.RUnlock()
}

func TestNativeStorageReset(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	insertionFilter := "model"
	macros := map[string]string{"foo": "bar"}
	insertionFilterExpr, _, err := storage.PrepareQuery(insertionFilter, macros)
	assert.Nil(t, err)
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	storage.Lock()
	storage.version = basenine.VERSION
	storage.partitionIndex = -1
	storage.macros = macros
	storage.insertionFilter = insertionFilter
	storage.insertionFilterExpr = insertionFilterExpr
	storage.Unlock()

	storage.InsertData([]byte(payload))

	storage.Reset()

	storage.RLock()
	assert.Equal(t, storage.version, basenine.VERSION)
	assert.Empty(t, storage.lastOffset)
	assert.Empty(t, storage.partitionRefs)
	assert.Empty(t, storage.offsets)
	assert.Len(t, storage.partitions, 1)
	assert.Empty(t, storage.partitionIndex)
	assert.Empty(t, storage.partitionSizeLimit)
	assert.Empty(t, storage.truncatedTimestamp)
	assert.Empty(t, storage.removedOffsetsCounter)
	assert.Empty(t, storage.macros)
	assert.Empty(t, storage.insertionFilter)
	assert.Empty(t, storage.insertionFilterExpr)
	storage.RUnlock()
}
