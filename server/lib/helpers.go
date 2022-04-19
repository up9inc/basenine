// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

import (
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

func IndexToID(index int) string {
	return fmt.Sprintf("%024d", index)
}

func SendOK(conn net.Conn) {
	SendMsg(conn, "OK")
}

func SendErr(conn net.Conn, err error) {
	if err != nil {
		SendMsg(conn, err.Error())
	}
}

func SendClose(conn net.Conn) {
	SendMsg(conn, CloseConnection)
}

func SendMsg(conn net.Conn, msg string) {
	conn.Write([]byte(fmt.Sprintf("%s\n", msg)))
}

// POSIX compliant method for checking whether connection was closed by the peer or not
func ConnCheck(conn net.Conn) error {
	var sysErr error = nil
	// Not easily testable through unit tests. net.Pipe() cannot be used directly.
	switch conn.(type) {
	case syscall.Conn:
		rc, err := conn.(syscall.Conn).SyscallConn()
		if err != nil {
			return err
		}
		err = rc.Read(func(fd uintptr) bool {
			var buf []byte = []byte{0}
			n, _, err := syscall.Recvfrom(int(fd), buf, syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
			switch {
			case n == 0 && err == nil:
				sysErr = io.EOF
			case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
				sysErr = nil
			default:
				sysErr = err
			}
			return true
		})
		if err != nil {
			return err
		}
	default:
		// Workaround for detecting close for net.Pipe()
		err := conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err == io.ErrClosedPipe {
			sysErr = err
		}
	}
	return sysErr
}

// check panics if the given error is not nil.
func Check(e error) {
	if e != nil {
		panic(e)
	}
}

// Reverses an int64 slice.
func ReverseSlice(arr []int64) (newArr []int64) {
	for i := len(arr) - 1; i >= 0; i-- {
		newArr = append(newArr, arr[i])
	}
	return newArr
}
