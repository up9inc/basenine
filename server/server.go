// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.
//
// Package main implements a schema-free, streaming database that
// also defines a TCP-based protocol. Please refer to the client libraries
// for communicating with the server.
//
// The server can be with a command like below:
//
//   basenine -addr -addr 127.0.0.1 -port 8000
//
// which sets the host address and TCP port.
//
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var addr = flag.String("addr", "", "The address to listen to; default is \"\" (all interfaces).")
var port = flag.Int("port", 8000, "The port to listen on; default is 8000.")

type ConnectionMode int

// The modes of TCP connections that the clients can use.
//
// INSERT is a long lasting TCP connection mode for inserting data into database.
//
// QUERY is a long lasting TCP connection mode for retrieving data from the database
// based on a given filtering query.
//
// SINGLE is a short lasting TCP connection mode for fetching a single record from the database.
//
// VALIDATE is a short lasting TCP connection mode for validating a query against syntax errors.
//
// MACRO is short lasting TCP connection mode for setting a macro that will be expanded
// later on for each individual query.
//
// LIMIT is short lasting TCP connection mode for setting the maximum database size
// to limit the disk usage.
const (
	NONE ConnectionMode = iota
	INSERT
	QUERY
	SINGLE
	VALIDATE
	MACRO
	LIMIT
)

type Commands int

// Commands refers to TCP connection modes.
const (
	CMD_INSERT   string = "/insert"
	CMD_QUERY    string = "/query"
	CMD_SINGLE   string = "/single"
	CMD_VALIDATE string = "/validate"
	CMD_MACRO    string = "/macro"
	CMD_LIMIT    string = "/limit"
)

// Constants defines the database filename's prefix and file extension.
const DB_FILE string = "data"
const DB_FILE_EXT string = "bin"

// Initial value of database size limit. 0 means unlimited size.
var dbSizeLimit int64 = 0

// Slice that stores the TCP connections
var connections []net.Conn

var cs ConcurrentSlice

// ConcurrentSlice is a mutually excluded struct that contains a list of fields that
// needs to be safely accessed data across multiple goroutines.
//
// lastOffset contains the offset of the latest inserted record into the database.
//
// partitionRefs is a slice that contains the corresponding partition references of offsets.
//
// offsets is a slice that contains the offsets of each individual records inserted into the database.
//
// partitions is a slice of file descriptors that refers to the database partitions.
//
// partitionIndex is the current index of currently being inserted database partition. Which
// is a reference to partitions slice's index and it's often times equal to len(partitions).
// Initial value of partitionIndex should be -1. -1 means there are no partitions yet.
type ConcurrentSlice struct {
	sync.RWMutex
	lastOffset     int64
	partitionRefs  []int64
	offsets        []int64
	partitions     []*os.File
	partitionIndex int64
}

// Main method that the program enters into
func main() {
	// Parse the command-line arguments.
	flag.Parse()

	fmt.Println("Starting server...")

	// Clean up the database files.
	removeDatabaseFiles()

	// Initialize the ConcurrentSlice.
	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	// Start listenning to given address and port.
	src := *addr + ":" + strconv.Itoa(*port)
	listener, _ := net.Listen("tcp", src)
	fmt.Printf("Listening on %s.\n", src)

	defer listener.Close()

	// Make a channel to gracefully close the TCP connections.
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Handle the channel.
	go func() {
		<-c
		quitConnections()
		removeDatabaseFiles()
		os.Exit(1)
	}()

	// Start accepting TCP connections.
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Some connection error: %s\n", err)
		}

		// Handle the TCP connection.
		go handleConnection(c, conn)
	}
}

// newParitition crates a new database paritition. The filename format is data_000000000.bin
// Such that the filename increments according to the partition index.
func newPartition() *os.File {
	cs.Lock()
	cs.partitionIndex++
	f, err := os.OpenFile(fmt.Sprintf("%s_%09d.%s", DB_FILE, cs.partitionIndex, DB_FILE_EXT), os.O_CREATE|os.O_WRONLY, 0644)
	check(err)
	cs.partitions = append(cs.partitions, f)
	cs.lastOffset = 0
	cs.Unlock()
	return f
}

// removeDatabaseFiles cleans up all of the database files.
func removeDatabaseFiles() {
	files, err := filepath.Glob("./data_*.bin")
	check(err)
	for _, f := range files {
		err = os.Remove(f)
		check(err)
	}
}

// periodicPartitioner is a Goroutine that handles database parititioning according
// to the database size limit that's set by /limit command.
// Triggered every second.
func periodicPartitioner(f *os.File) {
	for {
		time.Sleep(1 * time.Second)
		if dbSizeLimit == 0 {
			continue
		}
		info, err := f.Stat()
		check(err)
		currentSize := info.Size()
		if currentSize > dbSizeLimit {
			// If we exceeded the half of the database size limit, create a new partition
			f = newPartition()

			// Safely access the partitions slice and partitionIndex
			cs.Lock()
			if cs.partitionIndex > 1 {
				// There can be only two living partition any given time.
				// We've created the third partition, so discard the first one.
				discarded := cs.partitions[cs.partitionIndex-2]
				discarded.Close()
				os.Remove(discarded.Name())
			}
			cs.Unlock()
			check(err)
		}
	}
}

// periodicFileSyncer is a Goroutine that handles
// commiting the current contents of the file to stable storage
// by calling f.Sync() every 10 milliseconds.
func periodicFileSyncer(f *os.File) {
	for {
		time.Sleep(10 * time.Millisecond)
		cs.RLock()
		f = cs.partitions[cs.partitionIndex]
		cs.RUnlock()
		f.Sync()
	}
}

// handleConnection handles a TCP connection
func handleConnection(c chan os.Signal, conn net.Conn) {
	// Append connection into a global slice
	connections = append(connections, conn)

	// Log the connection
	remoteAddr := conn.RemoteAddr().String()
	fmt.Println("Client connected from " + remoteAddr)

	// Create a scanner
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)

	// Prevent buffer overflows
	scanner.Buffer(buf, 209715200)

	// Set connection mode to NONE
	var mode ConnectionMode = NONE
	var f *os.File

	defer f.Close()

	for {
		// Safely access the current partition index and get the current partition
		cs.RLock()
		if cs.partitionIndex > -1 {
			f = cs.partitions[cs.partitionIndex]
		}
		cs.RUnlock()

		// Scan the input
		ok := scanner.Scan()

		if !ok {
			err := scanner.Err()
			fmt.Printf("err: %v\n", err)
			break
		}

		// Handle the message
		_mode, data := handleMessage(scanner.Text(), conn)

		// Set the connection mode
		switch mode {
		case NONE:
			mode = _mode
			if mode == INSERT {
				f = newPartition()
				go periodicFileSyncer(f)
				go periodicPartitioner(f)
			}
		case INSERT:
			insertData(f, data)
		case QUERY:
			streamRecords(conn, data)
		case SINGLE:
			retrieveSingle(conn, data)
		case VALIDATE:
			validateQuery(conn, data)
		case MACRO:
			applyMacro(conn, data)
		case LIMIT:
			setLimit(conn, data)
		}
	}

	// Log the disconnect
	fmt.Println("Client at " + remoteAddr + " disconnected.")

	// In case of an INSERT mode client is disconnected, clean up the database files.
	if mode == INSERT {
		removeDatabaseFiles()
	}
}

// quitConnections quits all of the active TCP connections. It's only called
// in case of an interruption.
func quitConnections() {
	for _, conn := range connections {
		conn.Write([]byte("%quit%\n"))
	}
}

// handleMessage handles given message string of a TCP connection and returns a
// ConnectionMode to set the mode of the that TCP connection.
func handleMessage(message string, conn net.Conn) (mode ConnectionMode, data []byte) {
	fmt.Println("> " + message)

	if len(message) > 0 && message[0] == '/' {
		switch {
		case message == CMD_INSERT:
			mode = INSERT
			return

		case strings.HasPrefix(message, CMD_QUERY):
			mode = QUERY

		case message == CMD_SINGLE:
			mode = SINGLE

		case strings.HasPrefix(message, CMD_VALIDATE):
			mode = VALIDATE

		case strings.HasPrefix(message, CMD_MACRO):
			mode = MACRO

		case strings.HasPrefix(message, CMD_LIMIT):
			mode = LIMIT

		default:
			conn.Write([]byte("Unrecognized command.\n"))
		}
	} else {
		data = []byte(message)
	}

	return
}

// check throws a panic if the given error is not nil.
func check(e error) {
	if e != nil {
		panic(e)
	}
}

// insertData inserts a record into database.
// It unmarshals the given bytes into a map[string]interface{}
// Then inserts a key named "id" to that map. Which indicates the
// index of that record.
// Then marshals that map back and safely writes the bytes into
// the current database partitition.
func insertData(f *os.File, data []byte) {
	var d map[string]interface{}
	if err := json.Unmarshal(data, &d); err != nil {
		panic(err)
	}

	var lastOffset int64
	// Safely access the last offset
	cs.RLock()
	l := len(cs.offsets)
	lastOffset = cs.lastOffset
	cs.RUnlock()

	// TODO: Replace this with a substructure that serves as a metadata field.
	// Set "id" field to the index of the record.
	d["id"] = l

	// Marshal it back.
	data, _ = json.Marshal(d)

	// Calculate the length of bytes.
	var length int64 = int64(len(data))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(length))

	// Prepend the length into the data.
	data = append(b, data...)

	// Write the record into database immediately after the last record.
	// The offset is tracked by lastOffset which is cs.lastOffset
	// WriteAt() is important here! Write() races.
	n, err := f.WriteAt(data, lastOffset)
	check(err)

	// Log the amount of bytes that are written into the database.
	fmt.Printf("wrote %d bytes\n", n)

	// Safely update the offsets and paritition references.
	cs.Lock()
	cs.offsets = append(cs.offsets, lastOffset)
	cs.partitionRefs = append(cs.partitionRefs, cs.partitionIndex)
	cs.lastOffset = lastOffset + 8 + length
	cs.Unlock()
}

// readRecord reads the record from the database paritition provided by argument f
// and the reads the record by seeking to the offset provided by seek argument.
func readRecord(f *os.File, seek int64) (b []byte, n int64, err error) {
	n = seek
	l := make([]byte, 8)
	_, err = io.ReadAtLeast(f, l, 8)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return
	}
	n += 8
	check(err)
	length := int(binary.LittleEndian.Uint64(l))

	b = make([]byte, length)
	_, err = io.ReadAtLeast(f, b, length)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		n -= 8
		return
	}
	n += int64(length)
	check(err)
	return
}

// streamRecords is an infinite loop that only called in case of QUERY TCP connection mode.
// It expands marcros, parses the given query, does compile-time evaluations with Precompute() call
// and filters out the records according to query.
// It starts from the very beginning of the first living database partition.
// Means that either the current partition or the partition before that.
func streamRecords(conn net.Conn, data []byte) (err error) {
	query := string(data)

	// Expand all macros in the query, if there are any.
	query, err = expandMacros(query)
	check(err)

	// Parse the query.
	expr, err := Parse(query)
	check(err)

	// Do compile-time evaluations.
	err = Precompute(expr)
	check(err)

	// n is the last offset we were reading
	var n int64 = 0
	// -1 means that the paritition start index is not decided yet.
	var partitionStartIndex int64 = -1

	for {
		time.Sleep(10 * time.Millisecond)

		// Safely access the current partition index
		cs.Lock()
		partitionIndex := cs.partitionIndex
		cs.Unlock()

		// Paritition index -1 means there no partitions started yet.
		// Means that newPartition() is not called yet.
		if partitionIndex == -1 {
			continue
		}

		// This condition is only true if the previous partition EOL-ed
		// and the loop tries to continue with the next partition
		// but there are no new partitions yet.
		if partitionStartIndex > partitionIndex {
			partitionStartIndex = partitionIndex
		} else {
			// When the condition becomes false, it means that we switched
			// to the new partitition so we start to read from the
			// very first offset, which is 0.
			n = 0
		}

		// If the partitionStartIndex is still -1, means that we're still
		// in the very first iteration
		if partitionStartIndex == -1 {
			// Set partitionStartIndex to latest partition
			partitionStartIndex = partitionIndex
			// but if there is a partition before the latest one
			// set it to the previous partition.
			if partitionIndex > 1 {
				partitionStartIndex--
			}
		}

		// Safely access and open the partition decided by partitionStartIndex
		cs.Lock()
		f, err := os.Open(cs.partitions[partitionStartIndex].Name())
		cs.Unlock()

		// If the file cannot be opened, try again. The file should be there.
		if err != nil {
			continue
		}

		// Seek into the last offset that's tracked by n
		f.Seek(n, 0)

		// Read the records until seeing EOF
		for {
			var b []byte
			b, n, err = readRecord(f, n)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			// Evaluate the current record agains the given query.
			truth, err := Eval(expr, string(b))
			check(err)

			// Write the record into TCP connection if it passes the query.
			if truth {
				conn.Write([]byte(fmt.Sprintf("%s\n", b)))
			}
		}

		// Close the current partition.
		f.Close()

		// Increment the partition index tracked by the this loop.
		partitionStartIndex++
	}
}

// retrieveSingle fetches a single record from the database.
func retrieveSingle(conn net.Conn, data []byte) (err error) {
	// Convert index value provided as string to integer
	index, err := strconv.Atoi(string(data))
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the index to integer: %s\n", err.Error())))
		return
	}

	// Safely access the length of offsets slice.
	cs.RLock()
	l := len(cs.offsets)
	cs.RUnlock()

	// Check if the index is in the offsets slice.
	if index >= l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", index)))
		return
	}

	// Safely acces the offsets and partition references
	var f *os.File
	cs.RLock()
	n := cs.offsets[index]
	i := cs.partitionRefs[index]
	f, err = os.Open(cs.partitions[i].Name())
	cs.RUnlock()

	// Record can only be removed if the partition of the record
	// that it belongs to is removed. Therefore a file open error
	// means the record is removed.
	// It can only occur if the dbSizeLimit is set to a value
	// other than 0
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Record does not exist!\n")))
		return
	}

	// If we got to this point then it means the record is there
	// Read it using its offset (which is n) and return it.
	f.Seek(n, 0)
	var b []byte
	b, n, err = readRecord(f, n)
	conn.Write([]byte(fmt.Sprintf("%s\n", b)))
	return
}

// validateQuery tries to parse the given query and checks if there are
// any syntax errors or not.
func validateQuery(conn net.Conn, data []byte) {
	query := string(data)
	_, err := Parse(query)

	if err == nil {
		conn.Write([]byte(fmt.Sprintf("OK\n")))
	} else {
		conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
	}
}

// applyMacro defines a macro that will be expanded for each individual query.
func applyMacro(conn net.Conn, data []byte) {
	str := string(data)

	s := strings.Split(str, "~")

	if len(s) != 2 {
		conn.Write([]byte("Error: Provide only two expressions!\n"))
	}

	macro := strings.TrimSpace(s[0])
	expanded := strings.TrimSpace(s[1])

	addMacro(macro, expanded)

	conn.Write([]byte(fmt.Sprintf("OK\n")))
}

// setLimit sets a limit for the maximum database size.
func setLimit(conn net.Conn, data []byte) {
	value, err := strconv.Atoi(string(data))

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the limit to integer: %s\n", err.Error())))
		return
	}

	dbSizeLimit = int64(value) / 2

	conn.Write([]byte(fmt.Sprintf("OK\n")))
}
