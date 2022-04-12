package basenine

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexToID(t *testing.T) {
	assert.Equal(t, "000000000000000000000042", IndexToID(42))
}

func TestSendX(t *testing.T) {
	server, client := net.Pipe()

	go func() {
		SendOK(client)
		SendErr(client, errors.New("example"))
		SendClose(client)
		server.Close()
	}()

	client.Close()
}

func TestServerConnCheck(t *testing.T) {
	server, client := net.Pipe()
	assert.Nil(t, ConnCheck(client))
	assert.Nil(t, ConnCheck(server))
	client.Close()
	assert.Equal(t, io.ErrClosedPipe, ConnCheck(client))
	server.Close()
	assert.Equal(t, io.ErrClosedPipe, ConnCheck(server))
}

func TestCheckError(t *testing.T) {
	assert.Panics(t, assert.PanicTestFunc(func() {
		Check(errors.New("something"))
	}))

	assert.NotPanics(t, assert.PanicTestFunc(func() {
		Check(nil)
	}))
}

func TestReverseSlice(t *testing.T) {
	in := []int64{1, 2, 3}
	out := []int64{3, 2, 1}
	_out := ReverseSlice(in)
	assert.Equal(t, out, _out)
}
