package main

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	for index := 0; index < 5; index++ {
		expected := fmt.Sprintf(`{"brand":{"name":"Chevrolet"},"id":%d,"model":"Camaro","year":2021}`, index)

		insertData(f, []byte(payload))

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
