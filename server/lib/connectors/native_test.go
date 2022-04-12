package connectors

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	basenine "github.com/up9inc/basenine/server/lib"
)

func TestServerNewPartition(t *testing.T) {
	connector := NewNativeConnector(false).(*nativeConnector)

	f := connector.newPartition()
	assert.NotNil(t, f)
	assert.FileExists(t, fmt.Sprintf("%s_%09d.%s", NATIVE_DB_FILE, connector.partitionIndex, NATIVE_DB_FILE_EXT))
}

func TestServerDumpRestoreCore(t *testing.T) {
	connector := NewNativeConnector(false).(*nativeConnector)

	err := connector.DumpCore(false, false)
	assert.Nil(t, err)

	err = connector.RestoreCore()
	assert.Nil(t, err)

	connector.removeDatabaseFiles()
}

func TestServerInsertAndReadData(t *testing.T) {
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	connector := NewNativeConnector(false).(*nativeConnector)

	f := connector.newPartition()
	assert.NotNil(t, f)

	for index := 0; index < 100; index++ {
		expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":"%s","model":"Camaro","year":2021}`, basenine.IndexToID(index))

		connector.InsertData([]byte(payload))

		// Safely acces the offsets and partition references
		n, rf, err := connector.getOffsetAndPartition(index)
		assert.Nil(t, err)

		rf.Seek(n, io.SeekStart)
		b, n, err := connector.readRecord(rf, n)
		assert.Nil(t, err)
		assert.Greater(t, n, int64(0))
		assert.JSONEq(t, expected, string(b))

		rf.Close()
	}

	connector.removeDatabaseFiles()
}

func TestServerFlush(t *testing.T) {
	connector := NewNativeConnector(false).(*nativeConnector)

	insertionFilter := "model"
	insertionFilterExpr, _, err := connector.PrepareQuery(insertionFilter)
	assert.Nil(t, err)
	macros := map[string]string{"foo": "bar"}
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	connector.Lock()
	connector.version = basenine.VERSION
	connector.partitionIndex = -1
	connector.macros = macros
	connector.insertionFilter = insertionFilter
	connector.insertionFilterExpr = insertionFilterExpr
	connector.Unlock()

	connector.InsertData([]byte(payload))

	connector.Flush()

	connector.RLock()
	assert.Equal(t, connector.version, basenine.VERSION)
	assert.Empty(t, connector.lastOffset)
	assert.Empty(t, connector.partitionRefs)
	assert.Empty(t, connector.offsets)
	assert.Len(t, connector.partitions, 1)
	assert.Empty(t, connector.partitionIndex)
	assert.Empty(t, connector.partitionSizeLimit)
	assert.Empty(t, connector.truncatedTimestamp)
	assert.Empty(t, connector.removedOffsetsCounter)
	assert.Equal(t, connector.macros, macros)
	assert.Equal(t, connector.insertionFilter, insertionFilter)
	assert.Equal(t, connector.insertionFilterExpr, insertionFilterExpr)
	connector.RUnlock()
}

func TestServerReset(t *testing.T) {
	connector := NewNativeConnector(false).(*nativeConnector)

	insertionFilter := "model"
	insertionFilterExpr, _, err := connector.PrepareQuery(insertionFilter)
	assert.Nil(t, err)
	macros := map[string]string{"foo": "bar"}
	payload := `{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`

	connector.Lock()
	connector.version = basenine.VERSION
	connector.partitionIndex = -1
	connector.macros = macros
	connector.insertionFilter = insertionFilter
	connector.insertionFilterExpr = insertionFilterExpr
	connector.Unlock()

	connector.InsertData([]byte(payload))

	connector.Reset()

	connector.RLock()
	assert.Equal(t, connector.version, basenine.VERSION)
	assert.Empty(t, connector.lastOffset)
	assert.Empty(t, connector.partitionRefs)
	assert.Empty(t, connector.offsets)
	assert.Len(t, connector.partitions, 1)
	assert.Empty(t, connector.partitionIndex)
	assert.Empty(t, connector.partitionSizeLimit)
	assert.Empty(t, connector.truncatedTimestamp)
	assert.Empty(t, connector.removedOffsetsCounter)
	assert.Empty(t, connector.macros)
	assert.Empty(t, connector.insertionFilter)
	assert.Empty(t, connector.insertionFilterExpr)
	connector.RUnlock()
}
