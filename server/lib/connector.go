// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

import "net"

type Connector interface {
	DumpCore(silent bool, dontLock bool)
	RestoreCore() (err error)
	InsertData(data []byte)
	StreamRecords(conn net.Conn, data []byte) (err error)
	RetrieveSingle(conn net.Conn, args []string) (err error)
	Fetch(conn net.Conn, args []string)
	ApplyMacro(conn net.Conn, data []byte)
	SetLimit(conn net.Conn, data []byte)
	Flush()
	Reset()
}
