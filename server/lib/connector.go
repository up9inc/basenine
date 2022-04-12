// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

import (
	"net"
	"syscall"
)

type Connector interface {
	Init(persistent bool)
	DumpCore(silent bool, dontLock bool) (err error)
	RestoreCore() (err error)
	InsertData(data []byte)
	PrepareQuery(query string) (expr *Expression, prop Propagate, err error)
	StreamRecords(conn net.Conn, data []byte) (err error)
	RetrieveSingle(conn net.Conn, args []string) (err error)
	ValidateQuery(conn net.Conn, data []byte)
	Fetch(conn net.Conn, args []string)
	ApplyMacro(conn net.Conn, data []byte)
	SetLimit(conn net.Conn, data []byte)
	SetInsertionFilter(conn net.Conn, data []byte)
	Flush()
	Reset()
	HandleExit(sig syscall.Signal)
}
