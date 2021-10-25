// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.
//
// Package main implements a schema-free, streaming database that
// also defines a TCP-based protocol. Please refer to the client libraries
// for communicating with the server.
//
// The server can be run with a command like below:
//
//   basenine -addr -addr 127.0.0.1 -port 9099
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
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

var addr = flag.String("addr", "", "The address to listen to; default is \"\" (all interfaces).")
var port = flag.Int("port", 9099, "The port to listen on.")
var debug = flag.Bool("debug", false, "Enable debug logs.")
var version = flag.Bool("version", false, "Print version and exit.")

// Version of the software.
const VERSION string = "0.1.1"

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

// Global file watcher
var watcher *fsnotify.Watcher

func init() {
	// Clean up the database files.
	removeDatabaseFiles()

	// Initialize the ConcurrentSlice.
	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	// Trigger partitioning check for every second.
	ticker := time.NewTicker(1 * time.Second)
	go periodicPartitioner(ticker)

	// Initiate the global watcher
	var err error
	watcher, err = fsnotify.NewWatcher()
	check(err)
}

func main() {
	// Parse the command-line arguments.
	flag.Parse()

	// Print version and exit.
	if *version {
		fmt.Printf("%s\n", VERSION)
		os.Exit(0)
	}

	// Start listenning to given address and port.
	src := *addr + ":" + strconv.Itoa(*port)
	listener, err := net.Listen("tcp", src)
	check(err)
	log.Printf("Listening on %s\n", src)

	defer listener.Close()

	// Make a channel to gracefully close the TCP connections.
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Handle the channel.
	go func() {
		<-c
		quitConnections()
		watcher.Close()
		removeDatabaseFiles()
		os.Exit(1)
	}()

	// Start accepting TCP connections.
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection error: %s\n", err)
		}

		// Handle the TCP connection.
		go handleConnection(conn)
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

	err = watcher.Add(f.Name())
	check(err)

	return f
}

// removeDatabaseFiles cleans up all of the database files.
func removeDatabaseFiles() {
	files, err := filepath.Glob("./data_*.bin")
	check(err)
	for _, f := range files {
		os.Remove(f)
	}
}

// periodicPartitioner is a Goroutine that handles database parititioning according
// to the database size limit that's set by /limit command.
// Triggered every second.
func periodicPartitioner(ticker *time.Ticker) {
	var f *os.File
	for {
		<-ticker.C

		if dbSizeLimit == 0 {
			continue
		}

		// Safely access the current partition index and get the current partition
		cs.RLock()
		if cs.partitionIndex == -1 {
			cs.RUnlock()
			continue
		}
		f = cs.partitions[cs.partitionIndex]
		cs.RUnlock()

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
				err = watcher.Remove(discarded.Name())
				check(err)
				os.Remove(discarded.Name())
				cs.partitions[cs.partitionIndex-2] = nil
			}
			cs.Unlock()
			check(err)
		}
	}
}

// handleConnection handles a TCP connection
func handleConnection(conn net.Conn) {
	// Append connection into a global slice
	connections = append(connections, conn)

	// Log the connection
	remoteAddr := conn.RemoteAddr().String()
	log.Println("Client connected from " + remoteAddr)

	// Create a scanner
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)

	// Prevent buffer overflows
	scanner.Buffer(buf, 209715200)

	// Set connection mode to NONE
	var mode ConnectionMode = NONE

	for {
		// Scan the input
		ok := scanner.Scan()

		if !ok {
			if *debug {
				err := scanner.Err()
				log.Printf("Scanning error: %v\n", err)
			}
			break
		}

		// Handle the message
		_mode, data := handleMessage(scanner.Text(), conn)

		// Set the connection mode
		switch mode {
		case NONE:
			mode = _mode
			// partitionIndex -1 means there are not partitions created yet
			if mode == INSERT {
				// Safely access the current partition index
				cs.RLock()
				currentPartitionIndex := cs.partitionIndex
				cs.RUnlock()
				if currentPartitionIndex == -1 {
					newPartition()
				}
			}
		case INSERT:
			insertData(data)
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
	log.Println("Client at " + remoteAddr + " disconnected.")
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
	if *debug {
		log.Println("> " + message)
	}

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

// check panics if the given error is not nil.
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
func insertData(data []byte) {
	var d map[string]interface{}
	if err := json.Unmarshal(data, &d); err != nil {
		return
	}

	var lastOffset int64
	// Safely access the last offset and current partition.
	cs.Lock()
	l := len(cs.offsets)
	lastOffset = cs.lastOffset
	f := cs.partitions[cs.partitionIndex]

	// TODO: Replace this with a substructure that serves as a metadata field.
	// Set "id" field to the index of the record.
	d["id"] = l

	// Marshal it back.
	data, _ = json.Marshal(d)

	// Calculate the length of bytes.
	var length int64 = int64(len(data))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(length))

	// Safely update the offsets and paritition references.
	cs.offsets = append(cs.offsets, lastOffset)
	cs.partitionRefs = append(cs.partitionRefs, cs.partitionIndex)
	cs.lastOffset = lastOffset + 8 + length

	// Release the lock
	cs.Unlock()

	// Prepend the length into the data.
	data = append(b, data...)

	// Write the record into database immediately after the last record.
	// The offset is tracked by lastOffset which is cs.lastOffset
	// WriteAt() is important here! Write() races.
	n, err := f.WriteAt(data, lastOffset)
	check(err)

	if *debug {
		// Log the amount of bytes that are written into the database.
		log.Printf("Wrote %d bytes to the partition: %s\n", n, f.Name())
	}
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

// POSIX compliant method for checking whether connection was closed by the peer or not
func connCheck(conn net.Conn) error {
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
		err := conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		if err == io.ErrClosedPipe {
			sysErr = err
		}
	}
	return sysErr
}

// Blocks until a partition is modified
func watchPartitions() (err error) {
	select {
	case event, ok := <-watcher.Events:
		if !ok {
			return
		}
		if event.Op&fsnotify.Write != fsnotify.Write {
			return
		}
	case errW, ok := <-watcher.Errors:
		if !ok {
			err = errW
			return
		}
	}
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

	// The state to track the last offset's index in cs.offsets
	var leftOff int64 = 0

	for {
		// f is the current partition we're reading the data from.
		var f *os.File

		err = connCheck(conn)
		if err != nil {
			// Connection was closed by the peer, close the current partition.
			if f != nil {
				f.Close()
			}
			return
		}

		// Safely access the next part of offsets and partition references.
		cs.RLock()
		subOffsets := cs.offsets[leftOff:]
		subPartitionRefs := cs.partitionRefs[leftOff:]
		cs.RUnlock()

		// Iterate through the next part of the offsets
		for i, offset := range subOffsets {
			leftOff++

			// Safely access the *os.File pointer that the current offset refers to.
			cs.RLock()
			fRef := cs.partitions[subPartitionRefs[i]]
			cs.RUnlock()

			// File descriptor nil means; the partition is removed. So we pass this offset.
			if fRef == nil {
				continue
			}

			// f == nil means we didn't open any partition yet.
			// fRef.Name() != f.Name() means we're switching to the next partition.
			if f == nil || fRef.Name() != f.Name() {
				if f != nil && fRef.Name() != f.Name() {
					// We're switching to the next partition, close the current partition.
					f.Close()
				}

				// Open the partition that the current offset refers to.
				f, err = os.Open(fRef.Name())

				// If the file cannot be opened, pass.
				if err != nil {
					continue
				}
			}

			// Seek to the offset
			f.Seek(offset, io.SeekStart)

			// Read the record into b
			var b []byte
			b, _, err = readRecord(f, offset)

			// Even if it's EOF, continue.
			// Because a later offset might point to a previous region of the file.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				continue
			}

			// Evaluate the current record against the given query.
			truth, err := Eval(expr, string(b))
			check(err)

			// Write the record into TCP connection if it passes the query.
			if truth {
				_, err := conn.Write([]byte(fmt.Sprintf("%s\n", b)))
				if err != nil {
					log.Printf("Write error: %v\n", err)
					break
				}
			}
		}

		// Block until a partition is modified
		watchPartitions()
	}
}

// Safely acces the offsets and partition references
func getOffsetAndPartition(index int) (offset int64, f *os.File, err error) {
	cs.RLock()
	offset = cs.offsets[index]
	i := cs.partitionRefs[index]
	f, err = os.Open(cs.partitions[i].Name())
	cs.RUnlock()
	return
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
	n, f, err := getOffsetAndPartition(index)

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
	f.Seek(n, io.SeekStart)
	var b []byte
	b, _, err = readRecord(f, n)
	f.Close()
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
