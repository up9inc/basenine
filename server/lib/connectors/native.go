// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package connectors

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	jp "github.com/ohler55/ojg/jp"
	oj "github.com/ohler55/ojg/oj"
	basenine "github.com/up9inc/basenine/server/lib"
)

// Constants defines the database filename's prefix and file extension.
const NATIVE_DB_FILE string = "data"
const NATIVE_DB_FILE_LEGACY_EXT string = "bin"
const NATIVE_DB_FILE_EXT string = "db"

var nativeConnectorCoreDumpLock NativeConnectorCoreDumpLock

// nativeConnector is a mutually excluded struct that contains a list of fields that
// needs to be safely accessed data across multiple goroutines.
//
// version is the database server version.
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
//
// partitionSizeLimit is the value of database partition size limit. 0 means unlimited size.
//
// truncatedTimestamp is the timestamp of database truncation event upon size limiting.
//
// removedOffsetsCounter is the counter of how many offsets are removed through size limiting.
//
// macros is the map of strings where the key is the macro and value is the expanded form.
//
// insertionFilter is the filter that's applied just before the insertion of every individual record.
//
// insertionFilterExpr is the parsed version of insertionFilter
type nativeConnector struct {
	sync.RWMutex
	version               string
	lastOffset            int64
	partitionRefs         []int64
	offsets               []int64
	partitions            []*os.File
	partitionIndex        int64
	partitionSizeLimit    int64
	truncatedTimestamp    int64
	removedOffsetsCounter int
	macros                map[string]string
	insertionFilter       string
	insertionFilterExpr   *basenine.Expression
}

// Unmutexed, file descriptor clean version of nativeConnector for achieving core dump.
type nativeConnectorExport struct {
	Version               string
	LastOffset            int64
	PartitionRefs         []int64
	Offsets               []int64
	PartitionPaths        []string
	PartitionIndex        int64
	PartitionSizeLimit    int64
	TruncatedTimestamp    int64
	RemovedOffsetsCounter int
	Macros                map[string]string
	InsertionFilter       string
}

// Core dump filename
const coreDumpFilename string = "basenine.gob"
const coreDumpFilenameTemp string = "basenine_tmp.gob"

// Global file watcher
var watcher *fsnotify.Watcher

// Serves as a core dump lock
type NativeConnectorCoreDumpLock struct {
	sync.Mutex
}

var persistent bool
var debug bool

func NewNativeConnector(persistent bool) (connector basenine.Connector) {
	// Initialize the native connector.
	connector = &nativeConnector{
		version:        basenine.VERSION,
		partitionIndex: -1,
		macros:         make(map[string]string),
	}

	// Initialize the core dump lock.
	nativeConnectorCoreDumpLock = NativeConnectorCoreDumpLock{}

	// Initiate the global watcher
	var err error
	watcher, err = fsnotify.NewWatcher()
	basenine.Check(err)

	connector.Init(persistent)

	return
}

// Init initializes the connector
func (connector *nativeConnector) Init(persistent bool) {
	// Rename the legacy database files
	connector.renameLegacyDatabaseFiles()

	// If persistent mode is enabled, try to restore the core.
	var isRestored bool
	if persistent {
		err := connector.RestoreCore()
		if err == nil {
			isRestored = true
		}
	}

	if !isRestored {
		// Clean up the database files.
		connector.removeDatabaseFiles()
		connector.newPartition()
	}

	// Trigger partitioning check for every second.
	ticker := time.NewTicker(1 * time.Second)
	go connector.periodicPartitioner(ticker)
}

// DumpCore dumps the core into a file named "basenine.gob"
func (connector *nativeConnector) DumpCore(silent bool, dontLock bool) (err error) {
	nativeConnectorCoreDumpLock.Lock()
	var f *os.File
	f, err = os.Create(coreDumpFilenameTemp)
	if err != nil {
		return
	}
	defer f.Close()
	encoder := gob.NewEncoder(f)

	// nativeConnector has an embedded mutex. Therefore it cannot be dumped directly.
	var csExport nativeConnectorExport
	if !dontLock {
		connector.Lock()
	}
	csExport.Version = connector.version
	csExport.LastOffset = connector.lastOffset
	csExport.PartitionRefs = connector.partitionRefs
	csExport.Offsets = connector.offsets
	for _, partition := range connector.partitions {
		partitionPath := ""
		if partition != nil {
			partitionPath = partition.Name()
		}
		csExport.PartitionPaths = append(csExport.PartitionPaths, partitionPath)
	}
	csExport.PartitionIndex = connector.partitionIndex
	csExport.PartitionSizeLimit = connector.partitionSizeLimit
	csExport.TruncatedTimestamp = connector.truncatedTimestamp
	csExport.RemovedOffsetsCounter = connector.removedOffsetsCounter
	csExport.Macros = connector.macros
	csExport.InsertionFilter = connector.insertionFilter
	if !dontLock {
		connector.Unlock()
	}

	err = encoder.Encode(csExport)
	if err != nil {
		log.Printf("Error while dumping the core: %v\n", err.Error())
		return
	}

	os.Rename(coreDumpFilenameTemp, coreDumpFilename)

	if !silent {
		log.Printf("Dumped the core to: %s\n", coreDumpFilename)
	}
	nativeConnectorCoreDumpLock.Unlock()
	return
}

// RestoreCore restores the core from a file named "basenine.gob"
// if it's present in current working directory
func (connector *nativeConnector) RestoreCore() (err error) {
	var f *os.File
	f, err = os.Open(coreDumpFilename)
	if err != nil {
		log.Printf("Warning while restoring the core: %v\n", err)
		return
	}
	defer f.Close()
	decoder := gob.NewDecoder(f)

	var csExport nativeConnectorExport
	err = decoder.Decode(&csExport)
	if err != nil {
		log.Printf("Error while restoring the core: %v\n", err.Error())
		return
	}

	connector.version = basenine.VERSION
	connector.lastOffset = csExport.LastOffset
	connector.partitionRefs = csExport.PartitionRefs
	connector.offsets = csExport.Offsets
	for _, partitionPath := range csExport.PartitionPaths {
		if partitionPath == "" {
			connector.partitions = append(connector.partitions, nil)
			continue
		}
		var paritition *os.File
		paritition, err = os.OpenFile(partitionPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return
		}
		connector.partitions = append(connector.partitions, paritition)

		err = watcher.Add(paritition.Name())
		if err != nil {
			return
		}
	}
	connector.partitionIndex = csExport.PartitionIndex
	connector.partitionSizeLimit = csExport.PartitionSizeLimit
	connector.truncatedTimestamp = csExport.TruncatedTimestamp
	connector.removedOffsetsCounter = csExport.RemovedOffsetsCounter
	connector.macros = csExport.Macros
	connector.insertionFilter = csExport.InsertionFilter
	connector.insertionFilterExpr, _, _ = connector.PrepareQuery(connector.insertionFilter)

	log.Printf("Restored the core from: %s\n", coreDumpFilename)
	return
}

// InsertData inserts a record into database.
// It unmarshals the given bytes into a map[string]interface{}
// Then inserts a key named "id" to that map. Which indicates the
// index of that record.
// Then marshals that map back and safely writes the bytes into
// the current database partitition.
func (connector *nativeConnector) InsertData(data []byte) {
	// partitionIndex -1 means there are not partitions created yet
	// Safely access the current partition index
	connector.RLock()
	currentPartitionIndex := connector.partitionIndex
	connector.RUnlock()
	if currentPartitionIndex == -1 {
		connector.newPartition()
	}

	// Handle the insertion filter if it's not empty
	connector.RLock()
	insertionFilter := connector.insertionFilter
	insertionFilterExpr := connector.insertionFilterExpr
	connector.RUnlock()
	if len(insertionFilter) > 0 {
		truth, record, err := basenine.Eval(insertionFilterExpr, string(data))
		basenine.Check(err)
		if !truth {
			return
		}
		data = []byte(record)
	}

	var d map[string]interface{}
	if err := json.Unmarshal(data, &d); err != nil {
		return
	}

	var lastOffset int64
	// Safely access the last offset and current partition.
	connector.Lock()
	l := len(connector.offsets)
	lastOffset = connector.lastOffset
	f := connector.partitions[connector.partitionIndex]

	// Set "id" field to the index of the record.
	d["id"] = basenine.IndexToID(l)

	// Marshal it back.
	data, _ = json.Marshal(d)

	// Calculate the length of bytes.
	var length int64 = int64(len(data))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(length))

	// Safely update the offsets and paritition references.
	connector.offsets = append(connector.offsets, lastOffset)
	connector.partitionRefs = append(connector.partitionRefs, connector.partitionIndex)
	connector.lastOffset = lastOffset + 8 + length

	// Release the lock
	connector.Unlock()

	// Prepend the length into the data.
	data = append(b, data...)

	// Write the record into database immediately after the last record.
	// The offset is tracked by lastOffset which is connector.lastOffset
	// WriteAt() is important here! Write() races.
	n, err := f.WriteAt(data, lastOffset)
	basenine.Check(err)

	if debug {
		// Log the amount of bytes that are written into the database.
		log.Printf("Wrote %d bytes to the partition: %s\n", n, f.Name())
	}
}

// PrepareQuery get the query as an argument and handles expansion, parsing and compile-time evaluations.
func (connector *nativeConnector) PrepareQuery(query string) (expr *basenine.Expression, prop basenine.Propagate, err error) {
	// Expand all macros in the query, if there are any.
	connector.RLock()
	macros := connector.macros
	connector.RUnlock()
	query, err = basenine.ExpandMacros(macros, query)
	basenine.Check(err)

	// Parse the query.
	expr, err = basenine.Parse(query)
	if err != nil {
		log.Printf("Syntax error: %v\n", err)
		return
	}

	// leftOff is the state to track the last offset's index in connector.offsets
	// default value of leftOff is 0. leftOff(..) helper overrides it.
	// can be -1 also, means that it's last record.
	prop, err = basenine.Precompute(expr)
	basenine.Check(err)

	return
}

// StreamRecords is an infinite loop that only called in case of QUERY TCP connection mode.
// It expands marcros, parses the given query, does compile-time evaluations with Precompute() call
// and filters out the records according to query.
// It starts from the very beginning of the first living database partition.
// Means that either the current partition or the partition before that.
func (connector *nativeConnector) StreamRecords(conn net.Conn, data []byte) (err error) {
	expr, prop, err := connector.PrepareQuery(string(data))
	if err != nil {
		conn.Close()
		return
	}
	limit := prop.Limit
	rlimit := prop.Rlimit
	_leftOff := prop.LeftOff

	leftOff, err := connector.handleNegativeLeftOff(_leftOff)
	if err != nil {
		return
	}

	// Number of written records to the TCP connection.
	var numberOfWritten uint64 = 0

	// The queues for rlimit helper.
	var rlimitOffsetQueue []int64
	var rlimitPartitionRefQueue []int64
	if rlimit > 0 {
		rlimitOffsetQueue = make([]int64, 0)
		rlimitPartitionRefQueue = make([]int64, 0)
	}

	// Number of queried records
	var queried uint64 = 0

	// removedCounter keeps track of how many offsets belong to a removed partition.
	var removedOffsetsCounter int

	for {
		// f is the current partition we're reading the data from.
		var f *os.File

		err = basenine.ConnCheck(conn)
		if err != nil {
			// Connection was closed by the peer, close the current partition.
			if f != nil {
				f.Close()
			}
			return
		}

		// Safely access the next part of offsets and partition references.
		connector.RLock()
		subOffsets := connector.offsets[leftOff:]
		subPartitionRefs := connector.partitionRefs[leftOff:]
		totalNumberOfRecords := len(connector.offsets)
		truncatedTimestamp := connector.truncatedTimestamp
		removedOffsetsCounter = connector.removedOffsetsCounter
		connector.RUnlock()

		// Disable rlimit if it's bigger than the total records.
		if rlimit > 0 && int(rlimit) >= len(subOffsets) {
			rlimit = 0
		}

		var metadata *basenine.Metadata

		// Iterate through the next part of the offsets
		for i, offset := range subOffsets {
			leftOff++
			queried++

			// Safely access the *os.File pointer that the current offset refers to.
			var partitionRef int64
			connector.RLock()
			partitionRef = subPartitionRefs[i]
			fRef := connector.partitions[partitionRef]
			totalNumberOfRecords = len(connector.offsets)
			truncatedTimestamp = connector.truncatedTimestamp
			connector.RUnlock()

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
			b, _, err = connector.readRecord(f, offset)

			// Even if it's EOF, continue.
			// Because a later offset might point to a previous region of the file.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				continue
			}

			// Evaluate the current record against the given query.
			truth, record, err := basenine.Eval(expr, string(b))
			basenine.Check(err)

			// Write the record into TCP connection if it passes the query.
			if truth {
				if rlimit > 0 {
					rlimitOffsetQueue = append(rlimitOffsetQueue, offset)
					rlimitPartitionRefQueue = append(rlimitPartitionRefQueue, partitionRef)
				} else {
					_, err := conn.Write([]byte(fmt.Sprintf("%s\n", record)))
					if err != nil {
						log.Printf("Write error: %v\n", err)
						break
					}
					numberOfWritten++
				}
			}

			// Correct the metadata values by subtracting removedOffsetsCounter
			realTotal := totalNumberOfRecords - removedOffsetsCounter

			metadata = &basenine.Metadata{
				NumberOfWritten:    numberOfWritten,
				Current:            uint64(queried),
				Total:              uint64(realTotal),
				LeftOff:            basenine.IndexToID(int(leftOff)),
				TruncatedTimestamp: truncatedTimestamp,
			}
			queried = 0

			metadataMarshaled, _ := json.Marshal(metadata)
			_, err = conn.Write([]byte(fmt.Sprintf("%s %s\n", basenine.CMD_METADATA, string(metadataMarshaled))))
			if err != nil {
				log.Printf("Write error: %v\n", err)
				break
			}

			// If the number of written records is greater than or equal to the limit
			// and if the limit is not zero then stop the stream.
			if limit != 0 && numberOfWritten >= limit {
				return nil
			}
		}

		if rlimit > 0 {
			numberOfWritten, err = connector.rlimitWrite(conn, f, rlimit, rlimitOffsetQueue, rlimitPartitionRefQueue, numberOfWritten)
			rlimit = 0
		}

		// Block until a partition is modified
		connector.watchPartitions()
	}
}

// RetrieveSingle fetches a single record from the database.
func (connector *nativeConnector) RetrieveSingle(conn net.Conn, args []string) (err error) {
	// Convert index value provided as string to integer
	index, err := strconv.Atoi(args[0])
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the index to integer: %s\n", err.Error())))
		return
	}
	query := args[1]

	// Safely access the length of offsets slice.
	connector.RLock()
	l := len(connector.offsets)
	connector.RUnlock()

	// Check if the index is in the offsets slice.
	if index > l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", index)))
		return
	}

	// Safely acces the offsets and partition references
	n, f, err := connector.getOffsetAndPartition(index)

	// Record can only be removed if the partition of the record
	// that it belongs to is removed. Therefore a file open error
	// means the record is removed.
	// It can only occur if the partitionSizeLimit is set to a value
	// other than 0
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Record does not exist!\n")))
		return
	}

	// If we got to this point then it means the record is there
	// Read it using its offset (which is n) and return it.
	f.Seek(n, io.SeekStart)
	var b []byte
	b, _, err = connector.readRecord(f, n)
	f.Close()

	// Callling `Eval` for record altering helpers like `redact`
	expr, _, err := connector.PrepareQuery(query)
	if err != nil {
		conn.Close()
		return
	}
	_, record, err := basenine.Eval(expr, string(b))
	basenine.Check(err)

	conn.Write([]byte(fmt.Sprintf("%s\n", record)))
	return
}

// ValidateQuery tries to parse the given query and checks if there are
// any syntax errors or not.
func (connector *nativeConnector) ValidateQuery(conn net.Conn, data []byte) {
	query := string(data)
	// Expand all macros in the query, if there are any.
	connector.RLock()
	macros := connector.macros
	connector.RUnlock()
	query, err := basenine.ExpandMacros(macros, query)
	basenine.Check(err)
	_, err = basenine.Parse(query)

	if err == nil {
		basenine.SendOK(conn)
	} else {
		conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
	}
}

// Fetch fetches records in prefered direction, starting from leftOff up to given limit
func (connector *nativeConnector) Fetch(conn net.Conn, args []string) {
	// Parse the arguments
	_leftOff := args[0]
	leftOff, err := connector.handleNegativeLeftOff(_leftOff)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: Cannot parse leftOff value to int: %s\n", err.Error())))
		return
	}

	direction, err := strconv.Atoi(args[1])
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the direction to integer: %s\n", err.Error())))
		return
	}
	query := args[2]
	limit, err := strconv.Atoi(args[3])
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the limit to integer: %s\n", err.Error())))
		return
	}

	if direction < 0 {
		if leftOff > 0 {
			leftOff--
		}
	} else {
		leftOff++
	}

	// Safely access the length of offsets slice.
	connector.RLock()
	l := len(connector.offsets)
	connector.RUnlock()

	// Check if the leftOff is in the offsets slice.
	if int(leftOff) > l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", leftOff)))
		return
	}

	// `limit`, `rlimit` and `leftOff` helpers are not effective in `FETCH` connection mode
	expr, _, err := connector.PrepareQuery(query)
	if err != nil {
		conn.Close()
	}

	// Number of written records to the TCP connection.
	var numberOfWritten uint64 = 0

	// f is the current partition we're reading the data from.
	var f *os.File

	err = basenine.ConnCheck(conn)
	if err != nil {
		// Connection was closed by the peer, close the current partition.
		if f != nil {
			f.Close()
		}
		return
	}

	// Safely access the next part of offsets and partition references.
	var subOffsets []int64
	var subPartitionRefs []int64
	var totalNumberOfRecords int
	var truncatedTimestamp int64
	var removedOffsetsCounter int
	connector.RLock()
	totalNumberOfRecords = len(connector.offsets)
	truncatedTimestamp = connector.truncatedTimestamp
	if direction < 0 {
		subOffsets = connector.offsets[:leftOff]
		subPartitionRefs = connector.partitionRefs[:leftOff]
	} else {
		subOffsets = connector.offsets[leftOff:]
		subPartitionRefs = connector.partitionRefs[leftOff:]
	}
	removedOffsetsCounter = connector.removedOffsetsCounter
	connector.RUnlock()

	var metadata []byte

	// Number of queried records
	var queried uint64 = 0

	metadata, _ = json.Marshal(basenine.Metadata{
		NumberOfWritten:    numberOfWritten,
		Current:            uint64(queried),
		Total:              uint64(totalNumberOfRecords - removedOffsetsCounter),
		LeftOff:            basenine.IndexToID(int(leftOff)),
		TruncatedTimestamp: truncatedTimestamp,
	})

	if direction < 0 {
		subOffsets = basenine.ReverseSlice(subOffsets)
		subPartitionRefs = basenine.ReverseSlice(subPartitionRefs)
	}

	// Iterate through the next part of the offsets
	for i, offset := range subOffsets {
		if int(numberOfWritten) >= limit {
			return
		}

		if direction < 0 {
			leftOff--
		} else {
			leftOff++
		}

		if leftOff < 0 {
			leftOff = 0
		}

		queried++

		// Safely access the *os.File pointer that the current offset refers to.
		var partitionRef int64
		connector.RLock()
		partitionRef = subPartitionRefs[i]
		fRef := connector.partitions[partitionRef]
		connector.RUnlock()

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
		b, _, err = connector.readRecord(f, offset)

		// Even if it's EOF, continue.
		// Because a later offset might point to a previous region of the file.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			continue
		}

		// Evaluate the current record against the given query.
		truth, record, err := basenine.Eval(expr, string(b))
		basenine.Check(err)

		metadata, _ = json.Marshal(basenine.Metadata{
			NumberOfWritten:    numberOfWritten,
			Current:            uint64(queried),
			Total:              uint64(totalNumberOfRecords - removedOffsetsCounter),
			LeftOff:            basenine.IndexToID(int(leftOff)),
			TruncatedTimestamp: truncatedTimestamp,
		})

		_, err = conn.Write([]byte(fmt.Sprintf("%s %s\n", basenine.CMD_METADATA, string(metadata))))
		if err != nil {
			log.Printf("Write error: %v\n", err)
			break
		}

		// Write the record into TCP connection if it passes the query.
		if truth {
			_, err := conn.Write([]byte(fmt.Sprintf("%s\n", record)))
			if err != nil {
				log.Printf("Write error: %v\n", err)
				break
			}
			numberOfWritten++
		}
	}
}

// ApplyMacro defines a macro that will be expanded for each individual query.
func (connector *nativeConnector) ApplyMacro(conn net.Conn, data []byte) {
	str := string(data)

	s := strings.Split(str, "~")

	if len(s) != 2 {
		conn.Write([]byte("Error: Provide only two expressions!\n"))
		return
	}

	macro := strings.TrimSpace(s[0])
	expanded := strings.TrimSpace(s[1])

	connector.Lock()
	connector.macros = basenine.AddMacro(connector.macros, macro, expanded)
	connector.Unlock()

	basenine.SendOK(conn)
}

// SetLimit sets a limit for the maximum database size.
func (connector *nativeConnector) SetLimit(conn net.Conn, data []byte) {
	value, err := strconv.Atoi(string(data))

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the limit to integer: %s\n", err.Error())))
		return
	}

	connector.Lock()
	connector.partitionSizeLimit = int64(value) / 2
	connector.Unlock()

	basenine.SendOK(conn)
}

// SetInsertionFilter tries to set the given query as an insertion filter
func (connector *nativeConnector) SetInsertionFilter(conn net.Conn, data []byte) {
	query := string(data)

	insertionFilterExpr, _, err := connector.PrepareQuery(query)

	if err == nil {
		connector.Lock()
		connector.insertionFilter = query
		connector.insertionFilterExpr = insertionFilterExpr
		connector.Unlock()
		basenine.SendOK(conn)
	} else {
		conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
	}
}

// Flush removes all the records in the database.
func (connector *nativeConnector) Flush() {
	connector.Lock()
	connector.removeAllWatchers()
	connector.lastOffset = 0
	connector.partitionRefs = []int64{}
	connector.offsets = []int64{}
	connector.partitions = []*os.File{}
	connector.partitionIndex = -1
	connector.partitionSizeLimit = 0
	connector.truncatedTimestamp = 0
	connector.removedOffsetsCounter = 0
	connector.removeDatabaseFiles()
	connector.DumpCore(true, true)
	connector.Unlock()
	connector.newPartition()
}

// Reset removes all the records in the database and
// resets the core's state into its initial form.
func (connector *nativeConnector) Reset() {
	connector.Lock()
	connector.removeAllWatchers()
	connector.version = basenine.VERSION
	connector.macros = make(map[string]string)
	connector.insertionFilter = ""
	connector.insertionFilterExpr = nil
	connector.lastOffset = 0
	connector.partitionRefs = []int64{}
	connector.offsets = []int64{}
	connector.partitions = []*os.File{}
	connector.partitionIndex = -1
	connector.partitionSizeLimit = 0
	connector.truncatedTimestamp = 0
	connector.removedOffsetsCounter = 0
	connector.removeDatabaseFiles()
	connector.DumpCore(true, true)
	connector.Unlock()
	connector.newPartition()
}

// HandleExit gracefully exists the server accordingly. Dumps core if "-persistent" enabled.
func (connector *nativeConnector) HandleExit(sig syscall.Signal) {
	watcher.Close()

	// 128: killed by a signal and dumped core
	// + the signal value.
	exitCode := int(128 + sig)

	if !persistent {
		connector.removeDatabaseFiles()
		os.Exit(exitCode)
	}

	connector.DumpCore(false, false)

	os.Exit(exitCode)
}

// newPartition crates a new database paritition. The filename format is data_000000000.db
// Such that the filename increments according to the partition index.
func (connector *nativeConnector) newPartition() *os.File {
	connector.Lock()
	connector.partitionIndex++
	f, err := os.OpenFile(fmt.Sprintf("%s_%09d.%s", NATIVE_DB_FILE, connector.partitionIndex, NATIVE_DB_FILE_EXT), os.O_CREATE|os.O_WRONLY, 0644)
	basenine.Check(err)
	connector.partitions = append(connector.partitions, f)
	connector.lastOffset = 0
	connector.Unlock()

	err = watcher.Add(f.Name())
	basenine.Check(err)

	return f
}

// removeDatabaseFiles cleans up all of the database files.
func (connector *nativeConnector) removeDatabaseFiles() {
	files, err := filepath.Glob(fmt.Sprintf("./data_*.%s", NATIVE_DB_FILE_EXT))
	basenine.Check(err)
	for _, f := range files {
		os.Remove(f)
	}
}

// renameLegacyDatabaseFiles cleans up all of the database files.
func (connector *nativeConnector) renameLegacyDatabaseFiles() {
	files, err := filepath.Glob(fmt.Sprintf("./data_*.%s", NATIVE_DB_FILE_LEGACY_EXT))
	basenine.Check(err)
	for _, infile := range files {
		ext := path.Ext(infile)
		outfile := infile[0:len(infile)-len(ext)] + "." + NATIVE_DB_FILE_EXT
		os.Rename(infile, outfile)
	}
}

func (connector *nativeConnector) getLastTimestampOfPartition(discardedPartitionIndex int64) (timestamp int64, err error) {
	connector.RLock()
	offsets := connector.offsets
	partitionRefs := connector.partitionRefs
	connector.RUnlock()

	var prevIndex int
	var removedOffsetsCounter int
	for i := range offsets {
		if partitionRefs[i] > discardedPartitionIndex {
			break
		}
		prevIndex = i
		removedOffsetsCounter++
	}

	connector.Lock()
	connector.removedOffsetsCounter = removedOffsetsCounter
	connector.Unlock()

	var n int64
	var f *os.File
	n, f, err = connector.getOffsetAndPartition(prevIndex)

	if err != nil {
		return
	}

	f.Seek(n, io.SeekStart)
	var b []byte
	b, _, err = connector.readRecord(f, n)
	f.Close()

	var jsonPath jp.Expr
	jsonPath, err = jp.ParseString(`timestamp`)

	if err != nil {
		return
	}

	obj, err := oj.ParseString(string(b))
	if err != nil {
		return
	}

	result := jsonPath.Get(obj)

	if len(result) < 1 {
		err = errors.New("JSONPath could not be found!")
		return
	}

	timestamp = result[0].(int64)
	return
}

// periodicPartitioner is a Goroutine that handles database parititioning according
// to the database size limit that's set by /limit command.
// Triggered every second.
func (connector *nativeConnector) periodicPartitioner(ticker *time.Ticker) {
	var f *os.File
	for {
		<-ticker.C

		if persistent {
			// Dump the core periodically
			connector.DumpCore(true, false)
		}

		var partitionSizeLimit int64

		// Safely access the partition size limit, current partition index and get the current partition
		connector.RLock()
		partitionSizeLimit = connector.partitionSizeLimit
		if partitionSizeLimit == 0 || connector.partitionIndex == -1 {
			connector.RUnlock()
			continue
		}
		f = connector.partitions[connector.partitionIndex]
		connector.RUnlock()

		info, err := f.Stat()
		basenine.Check(err)
		currentSize := info.Size()
		if currentSize > partitionSizeLimit {
			// If we exceeded the half of the database size limit, create a new partition
			f = connector.newPartition()

			// Safely access the partitions slice and partitionIndex
			if connector.partitionIndex > 1 {
				// Populate the truncatedTimestamp field, which symbolizes the new
				// recording start time
				var truncatedTimestamp int64
				truncatedTimestamp, err = connector.getLastTimestampOfPartition(connector.partitionIndex - 2)
				if err == nil {
					connector.truncatedTimestamp = truncatedTimestamp + 1
				}

				connector.Lock()
				// There can be only two living partition any given time.
				// We've created the third partition, so discard the first one.
				discarded := connector.partitions[connector.partitionIndex-2]
				discarded.Close()
				err = watcher.Remove(discarded.Name())
				if err != nil {
					log.Printf("Watch removal error: %v\n", err.Error())
				}
				os.Remove(discarded.Name())
				connector.partitions[connector.partitionIndex-2] = nil

				if persistent {
					// Dump the core in case of a partition removal
					connector.DumpCore(true, true)
				}
				connector.Unlock()
			}
		}
	}
}

// readRecord reads the record from the database paritition provided by argument f
// and the reads the record by seeking to the offset provided by seek argument.
func (connector *nativeConnector) readRecord(f *os.File, seek int64) (b []byte, n int64, err error) {
	n = seek
	l := make([]byte, 8)
	_, err = io.ReadAtLeast(f, l, 8)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return
	}
	n += 8
	basenine.Check(err)
	length := int(binary.LittleEndian.Uint64(l))

	b = make([]byte, length)
	_, err = io.ReadAtLeast(f, b, length)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		n -= 8
		return
	}
	n += int64(length)
	basenine.Check(err)
	return
}

// Blocks until a partition is modified
func (connector *nativeConnector) watchPartitions() (err error) {
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

// handleNegativeLeftOff handles negative leftOff value.
func (connector *nativeConnector) handleNegativeLeftOff(_leftOff string) (leftOff int64, err error) {
	// If leftOff value is -1 then set it to last offset
	if _leftOff == basenine.LATEST {
		connector.RLock()
		lastOffset := len(connector.offsets) - 1
		connector.RUnlock()
		leftOff = int64(lastOffset)
		if leftOff < 0 {
			leftOff = 0
		}
	} else if _leftOff != "" {
		var leftOffInt int
		leftOffInt, err = strconv.Atoi(_leftOff)
		leftOff = int64(leftOffInt)
	}

	return
}

// Safely access the offsets and partition references
func (connector *nativeConnector) getOffsetAndPartition(index int) (offset int64, f *os.File, err error) {
	connector.RLock()
	offset = connector.offsets[index]
	i := connector.partitionRefs[index]
	fRef := connector.partitions[i]
	if fRef == nil {
		err = errors.New("Read on not opened partition")
	} else {
		f, err = os.Open(fRef.Name())
	}
	connector.RUnlock()
	return
}

func (connector *nativeConnector) rlimitWrite(conn net.Conn, f *os.File, rlimit uint64, offsetQueue []int64, partitionRefQueue []int64, numberOfWritten uint64) (numberOfWrittenNew uint64, err error) {
	startIndex := len(offsetQueue) - int(rlimit)
	if startIndex < 0 {
		startIndex = 0
	}
	offsetQueue = offsetQueue[startIndex:]
	partitionRefQueue = partitionRefQueue[startIndex:]
	for i, offset := range offsetQueue {
		partitionRef := partitionRefQueue[i]

		connector.RLock()
		fRef := connector.partitions[partitionRef]
		connector.RUnlock()

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
		b, _, err = connector.readRecord(f, offset)

		// Even if it's EOF, continue.
		// Because a later offset might point to a previous region of the file.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			continue
		}

		_, err := conn.Write([]byte(fmt.Sprintf("%s\n", b)))
		if err != nil {
			log.Printf("Write error: %v\n", err)
			break
		}
		numberOfWritten++
	}
	numberOfWrittenNew = numberOfWritten
	return
}

// removeAllWatchers removes all the watchers that are watching the database files.
func (connector *nativeConnector) removeAllWatchers() {
	for _, partition := range connector.partitions {
		if partition == nil {
			continue
		}
		err := watcher.Remove(partition.Name())
		if err != nil {
			log.Printf("Watch removal error: %v\n", err.Error())
		}
	}
}
