package storages

import (
	"fmt"
	"io"
	"os"
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
		n, rf, err := storage.getOffsetAndPartition(index)
		assert.Nil(t, err)

		rf.Seek(n, io.SeekStart)
		b, n, err := storage.readRecord(rf, n)
		assert.Nil(t, err)
		assert.Greater(t, n, int64(0))
		assert.JSONEq(t, expected, string(b))

		rf.Close()
	}

	storage.removeDatabaseFiles()
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
