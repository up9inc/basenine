package storages

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	basenine "github.com/up9inc/basenine/server/lib"
)

func TestServerNewPartition(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	f := storage.newPartition()
	assert.NotNil(t, f)
	assert.FileExists(t, fmt.Sprintf("%s_%09d.%s", NATIVE_DB_FILE, storage.partitionIndex, NATIVE_DB_FILE_EXT))
}

func TestServerDumpRestoreCore(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	err := storage.DumpCore(false, false)
	assert.Nil(t, err)

	err = storage.RestoreCore()
	assert.Nil(t, err)

	storage.removeDatabaseFiles()
}

func TestServerInsertAndReadData(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	storage := NewNativeStorage(false).(*nativeStorage)

	f := storage.newPartition()
	assert.NotNil(t, f)

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

func TestServerFlush(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	insertionFilter := "model"
	insertionFilterExpr, _, err := storage.PrepareQuery(insertionFilter)
	assert.Nil(t, err)
	macros := map[string]string{"foo": "bar"}
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

func TestServerReset(t *testing.T) {
	storage := NewNativeStorage(false).(*nativeStorage)

	insertionFilter := "model"
	insertionFilterExpr, _, err := storage.PrepareQuery(insertionFilter)
	assert.Nil(t, err)
	macros := map[string]string{"foo": "bar"}
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
