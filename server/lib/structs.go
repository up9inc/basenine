// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

import (
	"net"
	"syscall"
)

// Version of the software.
const VERSION string = "0.7.3"

type ConnectionMode int

// The modes of TCP connections that the clients can use.
//
// INSERT is a long lasting TCP connection mode for inserting data into database.
//
// INSERTION_FILTER is a short lasting TCP connection mode for setting the insertion filter.
//
// QUERY is a long lasting TCP connection mode for retrieving data from the database
// based on a given filtering query.
//
// SINGLE is a short lasting TCP connection mode for fetching a single record from the database.
//
// FETCH is a short lasting TCP connection mode for fetching N number of records from the database,
// starting from a certain offset, supporting both directions.
//
// VALIDATE is a short lasting TCP connection mode for validating a query against syntax errors.
//
// MACRO is a short lasting TCP connection mode for setting a macro that will be expanded
// later on for each individual query.
//
// LIMIT is a short lasting TCP connection mode for setting the maximum database size
// to limit the disk usage.
//
// FLUSH is a short lasting TCP connection mode that removes all the records in the database.
//
// RESET is a short lasting TCP connection mode that removes all the records in the database
// and resets the core into its initial state.
const (
	NONE ConnectionMode = iota
	INSERT
	INSERTION_FILTER
	QUERY
	SINGLE
	FETCH
	VALIDATE
	MACRO
	LIMIT
	FLUSH
	RESET
)

type Commands int

// Commands refers to TCP connection modes.
const (
	CMD_INSERT           string = "/insert"
	CMD_INSERTION_FILTER string = "/insert-filter"
	CMD_QUERY            string = "/query"
	CMD_SINGLE           string = "/single"
	CMD_FETCH            string = "/fetch"
	CMD_VALIDATE         string = "/validate"
	CMD_MACRO            string = "/macro"
	CMD_LIMIT            string = "/limit"
	CMD_METADATA         string = "/metadata"
	CMD_FLUSH            string = "/flush"
	CMD_RESET            string = "/reset"
)

// Metadata info that's streamed after each record
type Metadata struct {
	Current            uint64 `json:"current"`
	Total              uint64 `json:"total"`
	NumberOfWritten    uint64 `json:"numberOfWritten"`
	LeftOff            string `json:"leftOff"`
	TruncatedTimestamp int64  `json:"truncatedTimestamp"`
	NoMoreData         bool   `json:"noMoreData"`
}

// Closing indicators
const (
	CloseConnection = "%quit%"
)

// The interface for all of the different storage solutions.
type Storage interface {
	Init(persistent bool) (err error)
	DumpCore(silent bool, dontLock bool) (err error)
	RestoreCore() (err error)
	InsertData(data []byte) (insertedId interface{}, err error)
	ValidateQuery(conn net.Conn, data []byte) (err error)
	GetMacros() (macros map[string]string, err error)
	PrepareQuery(query string, macros map[string]string) (expr *Expression, prop Propagate, err error)
	StreamRecords(conn net.Conn, query string, fetch string, timeoutMs string) (err error)
	RetrieveSingle(conn net.Conn, index string, query string) (err error)
	Fetch(conn net.Conn, leftOff string, direction string, query string, limit string) (err error)
	ApplyMacro(conn net.Conn, data []byte) (err error)
	SetLimit(conn net.Conn, data []byte) (err error)
	SetInsertionFilter(conn net.Conn, data []byte) (err error)
	Flush() (err error)
	Reset() (err error)
	HandleExit(sig syscall.Signal, persistent bool) (err error)
}
