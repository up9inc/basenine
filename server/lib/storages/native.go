// Copyright 2022 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package storages

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
const NATIVE_STORAGE_DB_FILE string = "data"
const NATIVE_STORAGE_DB_FILE_LEGACY_EXT string = "bin"
const NATIVE_STORAGE_DB_FILE_EXT string = "db"

var nativeStorageCoreDumpLock NativeStorageCoreDumpLock

// nativeStorage is a mutually excluded struct that contains a list of fields that
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
type nativeStorage struct {
	sync.RWMutex
	version               string
	lastOffset            int64
	partitionRefs         []int64
	offsets               []int64
	partitions            []*os.File
	partitionIndex        int64
	partitionSizeLimit    int64
	truncatedTimestamp    int64
	removedOffsetsCounter uint64
	macros                map[string]string
	insertionFilter       string
	insertionFilterExpr   *basenine.Expression
	watcher               *fsnotify.Watcher
}

// Unmutexed, file descriptor clean version of nativeStorage for achieving core dump.
type nativeStorageExport struct {
	Version               string
	LastOffset            int64
	PartitionRefs         []int64
	Offsets               []int64
	PartitionPaths        []string
	PartitionIndex        int64
	PartitionSizeLimit    int64
	TruncatedTimestamp    int64
	RemovedOffsetsCounter uint64
	Macros                map[string]string
	InsertionFilter       string
}

// Core dump filename
const nativeStorageCoreDumpFilename string = "basenine.gob"
const nativeStorageCoreDumpFilenameTemp string = "basenine_tmp.gob"

// Serves as a core dump lock
type NativeStorageCoreDumpLock struct {
	sync.Mutex
}

func NewNativeStorage(persistent bool) (storage basenine.Storage) {
	// Initiate the watcher
	watcher, err := fsnotify.NewWatcher()
	basenine.Check(err)

	// Initialize the native storage.
	storage = &nativeStorage{
		version:        basenine.VERSION,
		partitionIndex: -1,
		macros:         make(map[string]string),
		watcher:        watcher,
	}

	// Initialize the core dump lock.
	nativeStorageCoreDumpLock = NativeStorageCoreDumpLock{}

	storage.Init(persistent)

	return
}

// Init initializes the storage
func (storage *nativeStorage) Init(persistent bool) (err error) {
	// Rename the legacy database files
	storage.renameLegacyDatabaseFiles()

	// If persistent mode is enabled, try to restore the core.
	var isRestored bool
	if persistent {
		err := storage.RestoreCore()
		if err == nil {
			isRestored = true
		}
	}

	if !isRestored {
		// Clean up the database files.
		storage.removeDatabaseFiles()
		storage.newPartition()
	}

	// Trigger partitioning check for every second.
	ticker := time.NewTicker(1 * time.Second)
	go storage.periodicPartitioner(persistent, ticker)
	return
}

// DumpCore dumps the core into a file named "basenine.gob"
func (storage *nativeStorage) DumpCore(silent bool, dontLock bool) (err error) {
	nativeStorageCoreDumpLock.Lock()
	var f *os.File
	f, err = os.Create(nativeStorageCoreDumpFilenameTemp)
	if err != nil {
		return
	}
	defer f.Close()
	encoder := gob.NewEncoder(f)

	// nativeStorage has an embedded mutex. Therefore it cannot be dumped directly.
	var csExport nativeStorageExport
	if !dontLock {
		storage.Lock()
	}
	csExport.Version = storage.version
	csExport.LastOffset = storage.lastOffset
	csExport.PartitionRefs = storage.partitionRefs
	csExport.Offsets = storage.offsets
	for _, partition := range storage.partitions {
		partitionPath := ""
		if partition != nil {
			partitionPath = partition.Name()
		}
		csExport.PartitionPaths = append(csExport.PartitionPaths, partitionPath)
	}
	csExport.PartitionIndex = storage.partitionIndex
	csExport.PartitionSizeLimit = storage.partitionSizeLimit
	csExport.TruncatedTimestamp = storage.truncatedTimestamp
	csExport.RemovedOffsetsCounter = storage.removedOffsetsCounter
	csExport.Macros = storage.macros
	csExport.InsertionFilter = storage.insertionFilter
	if !dontLock {
		storage.Unlock()
	}

	err = encoder.Encode(csExport)
	if err != nil {
		log.Printf("Error while dumping the core: %v\n", err.Error())
		return
	}

	os.Rename(nativeStorageCoreDumpFilenameTemp, nativeStorageCoreDumpFilename)

	if !silent {
		log.Printf("Dumped the core to: %s\n", nativeStorageCoreDumpFilename)
	}
	nativeStorageCoreDumpLock.Unlock()
	return
}

// RestoreCore restores the core from a file named "basenine.gob"
// if it's present in current working directory
func (storage *nativeStorage) RestoreCore() (err error) {
	var f *os.File
	f, err = os.Open(nativeStorageCoreDumpFilename)
	if err != nil {
		log.Printf("Warning while restoring the core: %v\n", err)
		return
	}
	defer f.Close()
	decoder := gob.NewDecoder(f)

	var csExport nativeStorageExport
	err = decoder.Decode(&csExport)
	if err != nil {
		log.Printf("Error while restoring the core: %v\n", err.Error())
		return
	}

	storage.Lock()
	storage.version = basenine.VERSION
	storage.lastOffset = csExport.LastOffset
	storage.partitionRefs = csExport.PartitionRefs
	storage.offsets = csExport.Offsets
	for _, partitionPath := range csExport.PartitionPaths {
		if partitionPath == "" {
			storage.partitions = append(storage.partitions, nil)
			continue
		}
		var paritition *os.File
		paritition, err = os.OpenFile(partitionPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return
		}
		storage.partitions = append(storage.partitions, paritition)

		err = storage.watcher.Add(paritition.Name())
		if err != nil {
			return
		}
	}
	storage.partitionIndex = csExport.PartitionIndex
	storage.partitionSizeLimit = csExport.PartitionSizeLimit
	storage.truncatedTimestamp = csExport.TruncatedTimestamp
	storage.removedOffsetsCounter = csExport.RemovedOffsetsCounter
	storage.macros = csExport.Macros
	storage.insertionFilter = csExport.InsertionFilter
	storage.insertionFilterExpr, _, _ = storage.PrepareQuery(storage.insertionFilter, csExport.Macros)
	storage.Unlock()

	log.Printf("Restored the core from: %s\n", nativeStorageCoreDumpFilename)
	return
}

// InsertData inserts a record into database.
// It unmarshals the given bytes into a map[string]interface{}
// Then inserts a key named "id" to that map. Which indicates the
// index of that record.
// Then marshals that map back and safely writes the bytes into
// the current database partitition.
func (storage *nativeStorage) InsertData(data []byte) (insertedId interface{}, err error) {
	// partitionIndex -1 means there are not partitions created yet
	// Safely access the current partition index
	storage.RLock()
	currentPartitionIndex := storage.partitionIndex
	storage.RUnlock()
	if currentPartitionIndex == -1 {
		storage.newPartition()
	}

	// Handle the insertion filter if it's not empty
	storage.RLock()
	insertionFilter := storage.insertionFilter
	insertionFilterExpr := storage.insertionFilterExpr
	storage.RUnlock()
	if len(insertionFilter) > 0 {
		var truth bool
		var record string
		truth, record, err = basenine.Eval(insertionFilterExpr, string(data))
		if err != nil {
			return
		}
		if !truth {
			return
		}
		data = []byte(record)
	}

	var d map[string]interface{}
	if err = json.Unmarshal(data, &d); err != nil {
		return
	}

	var lastOffset int64
	// Safely access the last offset and current partition.
	storage.Lock()
	l := len(storage.offsets) + int(storage.removedOffsetsCounter)
	lastOffset = storage.lastOffset
	f := storage.partitions[storage.partitionIndex]

	// Set "id" field to the index of the record.
	insertedId = basenine.IndexToID(l)
	d["id"] = insertedId

	// Marshal it back.
	data, _ = json.Marshal(d)

	// Calculate the length of bytes.
	var length int64 = int64(len(data))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(length))

	// Safely update the offsets and paritition references.
	storage.offsets = append(storage.offsets, lastOffset)
	storage.partitionRefs = append(storage.partitionRefs, storage.partitionIndex)
	storage.lastOffset = lastOffset + 8 + length

	// Release the lock
	storage.Unlock()

	// Prepend the length into the data.
	data = append(b, data...)

	// Write the record into database immediately after the last record.
	// The offset is tracked by lastOffset which is storage.lastOffset
	// WriteAt() is important here! Write() races.
	_, err = f.WriteAt(data, lastOffset)
	return
}

// GetMacros returns registered macros in the form a map of strings.
func (storage *nativeStorage) GetMacros() (macros map[string]string, err error) {
	storage.RLock()
	macros = storage.macros
	storage.RUnlock()
	return
}

// PrepareQuery get the query as an argument and handles expansion, parsing and compile-time evaluations.
func (storage *nativeStorage) PrepareQuery(query string, macros map[string]string) (expr *basenine.Expression, prop basenine.Propagate, err error) {
	// Expand all macros in the query, if there are any.
	query, err = basenine.ExpandMacros(macros, query)
	if err != nil {
		log.Printf("Macro expand error: %v\n", err)
		return
	}

	// Parse the query.
	expr, err = basenine.Parse(query)
	if err != nil {
		log.Printf("Syntax error: %v\n", err)
		return
	}

	prop, err = basenine.Precompute(expr)
	if err != nil {
		log.Printf("Precompute error: %v\n", err)
		return
	}

	return
}

// StreamRecords is an infinite loop that only called in case of QUERY TCP connection mode.
// It expands marcros, parses the given query, does compile-time evaluations with Precompute() call
// and filters out the records according to query.
// It starts from the very beginning of the first living database partition.
// Means that either the current partition or the partition before that.
func (storage *nativeStorage) StreamRecords(conn net.Conn, _leftOff string, query string) (err error) {
	var macros map[string]string
	macros, err = storage.GetMacros()
	if err != nil {
		conn.Close()
		return
	}

	var expr *basenine.Expression
	var prop basenine.Propagate
	expr, prop, err = storage.PrepareQuery(query, macros)
	if err != nil {
		conn.Close()
		return
	}

	limit := prop.Limit

	leftOff, err := storage.handleSpecialLeftOff(_leftOff, 1)
	if err != nil {
		return
	}

	// Number of written records to the TCP connection.
	var numberOfWritten uint64 = 0

	// Number of queried records
	var queried uint64 = 0

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
		storage.RLock()
		iLeftOff := leftOff - int64(storage.removedOffsetsCounter)
		if iLeftOff < 0 {
			leftOff = int64(storage.removedOffsetsCounter)
			iLeftOff = 0
		}
		subOffsets := storage.offsets[iLeftOff:]
		subPartitionRefs := storage.partitionRefs[iLeftOff:]
		totalNumberOfRecords := len(storage.offsets)
		truncatedTimestamp := storage.truncatedTimestamp
		storage.RUnlock()

		var metadata *basenine.Metadata

		// Iterate through the next part of the offsets
		for i, offset := range subOffsets {
			leftOff++
			queried++

			// Safely access the *os.File pointer that the current offset refers to.
			var partitionRef int64
			storage.RLock()
			partitionRef = subPartitionRefs[i]
			fRef := storage.partitions[partitionRef]
			totalNumberOfRecords = len(storage.offsets)
			truncatedTimestamp = storage.truncatedTimestamp
			storage.RUnlock()

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
			b, _, err = storage.readRecord(f, offset)

			// Even if it's EOF, continue.
			// Because a later offset might point to a previous region of the file.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				continue
			}

			// Evaluate the current record against the given query.
			truth, record, err := basenine.Eval(expr, string(b))
			if err != nil {
				log.Printf("Eval error: %v\n", err)
				continue
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

			metadata = &basenine.Metadata{
				NumberOfWritten:    numberOfWritten,
				Current:            uint64(queried),
				Total:              uint64(totalNumberOfRecords),
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

		// Block until a partition is modified
		storage.watchPartitions()
	}
}

// RetrieveSingle fetches a single record from the database.
func (storage *nativeStorage) RetrieveSingle(conn net.Conn, index string, query string) (err error) {
	// Convert index value provided as string to integer
	_index, err := strconv.Atoi(index)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the index to integer: %s\n", err.Error())))
		return
	}

	// Safely access the length of offsets slice.
	storage.RLock()
	l := len(storage.offsets) + int(storage.removedOffsetsCounter)
	removedOffsetsCounter := storage.removedOffsetsCounter
	storage.RUnlock()

	_index -= int(removedOffsetsCounter)

	if _index < 0 {
		conn.Write([]byte(fmt.Sprintf("Record does not exist!\n")))
		return
	}

	// Check if the index is in the offsets slice.
	if int(_index) > l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", _index)))
		return
	}

	// Safely acces the offsets and partition references
	n, f, err := storage.getOffsetAndPartition(uint64(_index))

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
	b, _, err = storage.readRecord(f, n)
	f.Close()
	if err != nil {
		msg := fmt.Sprintf("Read error: %v\n", err)
		log.Println(msg)
		conn.Write([]byte(msg))
		return
	}

	macros, err := storage.GetMacros()
	if err != nil {
		conn.Close()
		return
	}

	// Callling `Eval` for record altering helpers like `redact`
	expr, _, err := storage.PrepareQuery(query, macros)
	if err != nil {
		conn.Close()
		return
	}
	_, record, err := basenine.Eval(expr, string(b))
	if err != nil {
		msg := fmt.Sprintf("Eval error: %v\n", err)
		log.Println(msg)
		conn.Write([]byte(msg))
		return
	}

	conn.Write([]byte(fmt.Sprintf("%s\n", record)))
	return
}

// ValidateQuery tries to parse the given query and checks if there are
// any syntax errors or not.
func (storage *nativeStorage) ValidateQuery(conn net.Conn, query string) (err error) {
	// Expand all macros in the query, if there are any.
	storage.RLock()
	macros := storage.macros
	storage.RUnlock()
	query, err = basenine.ExpandMacros(macros, query)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
	}
	_, err = basenine.Parse(query)

	if err == nil {
		basenine.SendOK(conn)
	} else {
		conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
	}
	return
}

// Fetch fetches records in prefered direction, starting from leftOff up to given limit
func (storage *nativeStorage) Fetch(conn net.Conn, leftOff string, direction string, query string, limit string) (err error) {
	// Parse the arguments
	var _leftOff int64
	_leftOff, err = storage.handleSpecialLeftOff(leftOff, 0)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: Cannot parse leftOff value to int: %s\n", err.Error())))
		return
	}

	var _direction int
	_direction, err = strconv.Atoi(direction)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the direction to integer: %s\n", err.Error())))
		return
	}

	var _limit int
	_limit, err = strconv.Atoi(limit)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the limit to integer: %s\n", err.Error())))
		return
	}

	// Safely access the length of offsets slice.
	storage.RLock()
	l := len(storage.offsets) + int(storage.removedOffsetsCounter)
	storage.RUnlock()

	// Check if the leftOff is in the offsets slice.
	if int(_leftOff) > l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", _leftOff)))
		return
	}

	macros, err := storage.GetMacros()
	if err != nil {
		conn.Close()
		return
	}

	// `limit`, and `leftOff` helpers are not effective in `FETCH` connection mode
	var expr *basenine.Expression
	expr, _, err = storage.PrepareQuery(query, macros)
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
	var totalNumberOfRecords uint64
	var truncatedTimestamp int64
	storage.RLock()
	totalNumberOfRecords = uint64(len(storage.offsets))
	truncatedTimestamp = storage.truncatedTimestamp
	iLeftOff := _leftOff - int64(storage.removedOffsetsCounter)
	if iLeftOff < 0 {
		_leftOff = int64(storage.removedOffsetsCounter)
		iLeftOff = 0
	}
	if _direction < 0 {
		subOffsets = storage.offsets[:iLeftOff]
		subPartitionRefs = storage.partitionRefs[:iLeftOff]
	} else {
		subOffsets = storage.offsets[iLeftOff:]
		subPartitionRefs = storage.partitionRefs[iLeftOff:]
	}
	storage.RUnlock()

	var metadata []byte

	// Number of queried records
	var queried uint64 = 0

	metadata, _ = json.Marshal(basenine.Metadata{
		NumberOfWritten:    numberOfWritten,
		Current:            uint64(queried),
		Total:              totalNumberOfRecords,
		LeftOff:            basenine.IndexToID(int(_leftOff)),
		TruncatedTimestamp: truncatedTimestamp,
	})

	if _direction < 0 {
		subOffsets = basenine.ReverseSlice(subOffsets)
		subPartitionRefs = basenine.ReverseSlice(subPartitionRefs)
	}

	// Iterate through the next part of the offsets
	for i, offset := range subOffsets {
		if int(numberOfWritten) >= _limit {
			return
		}

		if _direction < 0 {
			_leftOff--
		} else {
			_leftOff++
		}

		if _leftOff < 0 {
			_leftOff = 0
		}

		queried++

		// Safely access the *os.File pointer that the current offset refers to.
		var partitionRef int64
		storage.RLock()
		partitionRef = subPartitionRefs[i]
		fRef := storage.partitions[partitionRef]
		storage.RUnlock()

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
		b, _, err = storage.readRecord(f, offset)

		// Even if it's EOF, continue.
		// Because a later offset might point to a previous region of the file.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			continue
		}

		// Evaluate the current record against the given query.
		truth, record, err := basenine.Eval(expr, string(b))
		if err != nil {
			log.Printf("Eval error: %v\n", err)
			continue
		}

		var noMoreData bool
		if i == len(subOffsets)-1 {
			noMoreData = true
		}

		metadata, _ = json.Marshal(basenine.Metadata{
			NumberOfWritten:    numberOfWritten,
			Current:            uint64(queried),
			Total:              totalNumberOfRecords,
			LeftOff:            basenine.IndexToID(int(_leftOff)),
			TruncatedTimestamp: truncatedTimestamp,
			NoMoreData:         noMoreData,
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

	basenine.SendClose(conn)
	return
}

// ApplyMacro defines a macro that will be expanded for each individual query.
func (storage *nativeStorage) ApplyMacro(conn net.Conn, data []byte) (err error) {
	str := string(data)

	s := strings.Split(str, "~")

	if len(s) != 2 {
		conn.Write([]byte("Error: Provide only two expressions!\n"))
		return
	}

	macro := strings.TrimSpace(s[0])
	expanded := strings.TrimSpace(s[1])

	storage.Lock()
	storage.macros = basenine.AddMacro(storage.macros, macro, expanded)
	storage.Unlock()

	basenine.SendOK(conn)
	return
}

// SetLimit sets a limit for the maximum database size.
func (storage *nativeStorage) SetLimit(conn net.Conn, data []byte) (err error) {
	value, err := strconv.Atoi(string(data))

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the limit to integer: %s\n", err.Error())))
		return
	}

	storage.setPartitionSizeLimit(value)

	basenine.SendOK(conn)
	return
}

// SetInsertionFilter tries to set the given query as an insertion filter
func (storage *nativeStorage) SetInsertionFilter(conn net.Conn, data []byte) (err error) {
	query := string(data)

	macros, err := storage.GetMacros()
	if err != nil {
		return
	}

	insertionFilterExpr, _, err := storage.PrepareQuery(query, macros)

	if err == nil {
		storage.Lock()
		storage.insertionFilter = query
		storage.insertionFilterExpr = insertionFilterExpr
		storage.Unlock()
		basenine.SendOK(conn)
	}
	return
}

// Flush removes all the records in the database.
func (storage *nativeStorage) Flush() (err error) {
	storage.Lock()
	storage.removeAllWatchers()
	storage.lastOffset = 0
	storage.partitionRefs = []int64{}
	storage.offsets = []int64{}
	storage.partitions = []*os.File{}
	storage.partitionIndex = -1
	storage.partitionSizeLimit = 0
	storage.truncatedTimestamp = 0
	storage.removedOffsetsCounter = 0
	storage.removeDatabaseFiles()
	storage.DumpCore(true, true)
	storage.Unlock()
	storage.newPartition()
	return
}

// Reset removes all the records in the database and
// resets the core's state into its initial form.
func (storage *nativeStorage) Reset() (err error) {
	storage.Lock()
	storage.removeAllWatchers()
	storage.version = basenine.VERSION
	storage.macros = make(map[string]string)
	storage.insertionFilter = ""
	storage.insertionFilterExpr = nil
	storage.lastOffset = 0
	storage.partitionRefs = []int64{}
	storage.offsets = []int64{}
	storage.partitions = []*os.File{}
	storage.partitionIndex = -1
	storage.partitionSizeLimit = 0
	storage.truncatedTimestamp = 0
	storage.removedOffsetsCounter = 0
	storage.removeDatabaseFiles()
	storage.DumpCore(true, true)
	storage.Unlock()
	storage.newPartition()
	return
}

// HandleExit gracefully exists the server accordingly. Dumps core if "-persistent" enabled.
func (storage *nativeStorage) HandleExit(sig syscall.Signal, persistent bool) (err error) {
	storage.watcher.Close()

	// 128: killed by a signal and dumped core
	// + the signal value.
	exitCode := int(128 + sig)

	if !persistent {
		storage.removeDatabaseFiles()
		os.Exit(exitCode)
	}

	storage.DumpCore(false, false)

	os.Exit(exitCode)
	return
}

// newPartition crates a new database paritition. The filename format is data_000000000.db
// Such that the filename increments according to the partition index.
func (storage *nativeStorage) newPartition() *os.File {
	storage.Lock()
	storage.partitionIndex++
	f, err := os.OpenFile(fmt.Sprintf("%s_%09d.%s", NATIVE_STORAGE_DB_FILE, storage.partitionIndex, NATIVE_STORAGE_DB_FILE_EXT), os.O_CREATE|os.O_WRONLY, 0644)
	basenine.Check(err)
	storage.partitions = append(storage.partitions, f)
	storage.lastOffset = 0
	storage.Unlock()

	err = storage.watcher.Add(f.Name())
	basenine.Check(err)

	return f
}

// removeDatabaseFiles cleans up all of the database files.
func (storage *nativeStorage) removeDatabaseFiles() {
	files, err := filepath.Glob(fmt.Sprintf("./data_*.%s", NATIVE_STORAGE_DB_FILE_EXT))
	basenine.Check(err)
	for _, f := range files {
		os.Remove(f)
	}
}

// renameLegacyDatabaseFiles cleans up all of the database files.
func (storage *nativeStorage) renameLegacyDatabaseFiles() {
	files, err := filepath.Glob(fmt.Sprintf("./data_*.%s", NATIVE_STORAGE_DB_FILE_LEGACY_EXT))
	basenine.Check(err)
	for _, infile := range files {
		ext := path.Ext(infile)
		outfile := infile[0:len(infile)-len(ext)] + "." + NATIVE_STORAGE_DB_FILE_EXT
		os.Rename(infile, outfile)
	}
}

func (storage *nativeStorage) getLastTimestampOfPartition(discardedPartitionIndex int64) (timestamp int64, err error) {
	storage.RLock()
	offsets := storage.offsets
	partitionRefs := storage.partitionRefs
	storage.RUnlock()

	var removedOffsetsCounter uint64

	for i := range offsets {
		if partitionRefs[i] > discardedPartitionIndex {
			break
		}
		removedOffsetsCounter++
	}

	storage.Lock()
	storage.offsets = storage.offsets[removedOffsetsCounter:]
	storage.partitionRefs = storage.partitionRefs[removedOffsetsCounter:]
	storage.removedOffsetsCounter += removedOffsetsCounter
	storage.Unlock()

	var n int64
	var f *os.File
	n, f, err = storage.getOffsetAndPartition(0)

	if err != nil {
		return
	}

	f.Seek(n, io.SeekStart)
	var b []byte
	b, _, err = storage.readRecord(f, n)
	f.Close()
	if err != nil {
		return
	}

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
func (storage *nativeStorage) periodicPartitioner(persistent bool, ticker *time.Ticker) {
	var f *os.File
	for {
		<-ticker.C

		if persistent {
			// Dump the core periodically
			storage.DumpCore(true, false)
		}

		var partitionSizeLimit int64

		// Safely access the partition size limit, current partition index and get the current partition
		storage.RLock()
		partitionSizeLimit = storage.partitionSizeLimit
		if partitionSizeLimit == 0 || storage.partitionIndex == -1 {
			storage.RUnlock()
			continue
		}
		f = storage.partitions[storage.partitionIndex]
		storage.RUnlock()

		info, err := f.Stat()
		basenine.Check(err)
		currentSize := info.Size()
		if currentSize > partitionSizeLimit {
			// If we exceeded the half of the database size limit, create a new partition
			f = storage.newPartition()

			// Safely access the partitions slice and partitionIndex
			if storage.partitionIndex > 1 {
				// Populate the truncatedTimestamp field, which symbolizes the new
				// recording start time
				var truncatedTimestamp int64
				truncatedTimestamp, err = storage.getLastTimestampOfPartition(storage.partitionIndex - 2)
				if err == nil {
					storage.truncatedTimestamp = truncatedTimestamp + 1
				}

				storage.Lock()
				// There can be only two living partition any given time.
				// We've created the third partition, so discard the first one.
				discarded := storage.partitions[storage.partitionIndex-2]
				discarded.Close()
				err = storage.watcher.Remove(discarded.Name())
				if err != nil {
					log.Printf("Watch removal error: %v\n", err.Error())
				}
				os.Remove(discarded.Name())
				storage.partitions[storage.partitionIndex-2] = nil

				if persistent {
					// Dump the core in case of a partition removal
					storage.DumpCore(true, true)
				}
				storage.Unlock()
			}
		}
	}
}

// readRecord reads the record from the database paritition provided by argument f
// and the reads the record by seeking to the offset provided by seek argument.
func (storage *nativeStorage) readRecord(f *os.File, seek int64) (b []byte, n int64, err error) {
	n = seek
	l := make([]byte, 8)
	_, err = io.ReadAtLeast(f, l, 8)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return
	}
	n += 8
	if err != nil {
		return
	}
	length := int(binary.LittleEndian.Uint64(l))

	b = make([]byte, length)
	_, err = io.ReadAtLeast(f, b, length)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		n -= 8
		return
	}
	n += int64(length)
	if err != nil {
		return
	}
	return
}

// Blocks until a partition is modified
func (storage *nativeStorage) watchPartitions() (err error) {
	select {
	case event, ok := <-storage.watcher.Events:
		if !ok {
			return
		}
		if event.Op&fsnotify.Write != fsnotify.Write {
			return
		}
	case errW, ok := <-storage.watcher.Errors:
		if !ok {
			err = errW
			return
		}
	}
	return
}

// handleSpecialLeftOff handles negative leftOff value.
func (storage *nativeStorage) handleSpecialLeftOff(_leftOff string, increment int64) (leftOff int64, err error) {
	// If leftOff value is -1 then set it to last offset
	if _leftOff == basenine.LATEST {
		storage.RLock()
		lastOffset := len(storage.offsets) + int(storage.removedOffsetsCounter) - 1
		storage.RUnlock()
		leftOff = int64(lastOffset)
		if leftOff < 0 {
			leftOff = 0
		}
	} else if _leftOff != "" {
		var leftOffInt int
		leftOffInt, err = strconv.Atoi(_leftOff)
		leftOff = int64(leftOffInt)
		leftOff += increment
	}

	return
}

// Safely access the offsets and partition references
func (storage *nativeStorage) getOffsetAndPartition(index uint64) (offset int64, f *os.File, err error) {
	storage.RLock()
	offset = storage.offsets[index]
	i := storage.partitionRefs[index]
	fRef := storage.partitions[i]
	if fRef == nil {
		err = errors.New("Read on not opened partition")
	} else {
		f, err = os.Open(fRef.Name())
	}
	storage.RUnlock()
	return
}

// removeAllWatchers removes all the watchers that are watching the database files.
func (storage *nativeStorage) removeAllWatchers() {
	for _, partition := range storage.partitions {
		if partition == nil {
			continue
		}
		err := storage.watcher.Remove(partition.Name())
		if err != nil {
			log.Printf("Watch removal error: %v\n", err.Error())
		}
	}
}

// setPartitionSizeLimit sets the partition size limit to the half of the given value.
func (storage *nativeStorage) setPartitionSizeLimit(value int) {
	storage.Lock()
	storage.partitionSizeLimit = int64(value) / 2
	storage.Unlock()
}
