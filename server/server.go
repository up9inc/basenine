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

const (
	CMD_INSERT   string = "/insert"
	CMD_QUERY    string = "/query"
	CMD_SINGLE   string = "/single"
	CMD_VALIDATE string = "/validate"
	CMD_MACRO    string = "/macro"
	CMD_LIMIT    string = "/limit"
)

const DB_FILE string = "data"
const DB_FILE_EXT string = "bin"

var dbSizeLimit int64 = 0

var connections []net.Conn

var cs ConcurrentSlice

type ConcurrentSlice struct {
	sync.RWMutex
	lastOffset     int64
	partitionRefs  []int64
	offsets        []int64
	partitions     []*os.File
	partitionIndex int64
}

func main() {
	flag.Parse()

	fmt.Println("Starting server...")
	removeDatabaseFiles()
	cs = ConcurrentSlice{
		partitionIndex: -1,
	}

	src := *addr + ":" + strconv.Itoa(*port)
	listener, _ := net.Listen("tcp", src)
	fmt.Printf("Listening on %s.\n", src)

	defer listener.Close()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		quitConnections()
		removeDatabaseFiles()
		os.Exit(1)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Some connection error: %s\n", err)
		}

		go handleConnection(c, conn)
	}
}

func newPartition() *os.File {
	cs.Lock()
	cs.partitionIndex += 1
	f, err := os.OpenFile(fmt.Sprintf("%s_%09d.%s", DB_FILE, cs.partitionIndex, DB_FILE_EXT), os.O_CREATE|os.O_WRONLY, 0644)
	check(err)
	cs.partitions = append(cs.partitions, f)
	cs.lastOffset = 0
	cs.Unlock()
	return f
}

func removeDatabaseFiles() {
	files, err := filepath.Glob("./data_*.bin")
	check(err)
	for _, f := range files {
		err = os.Remove(f)
		check(err)
	}
}

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
			f = newPartition()
			cs.Lock()
			if cs.partitionIndex > 1 {
				discarded := cs.partitions[cs.partitionIndex-2]
				discarded.Close()
				os.Remove(discarded.Name())
			}
			cs.Unlock()
			check(err)
		}
	}
}

func periodicFileSyncer(f *os.File) {
	for {
		time.Sleep(10 * time.Millisecond)
		cs.RLock()
		f = cs.partitions[cs.partitionIndex]
		cs.RUnlock()
		f.Sync()
	}
}

func handleConnection(c chan os.Signal, conn net.Conn) {
	connections = append(connections, conn)
	remoteAddr := conn.RemoteAddr().String()
	fmt.Println("Client connected from " + remoteAddr)

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 209715200)

	var mode ConnectionMode = NONE
	var f *os.File

	defer f.Close()

	for {
		cs.RLock()
		if cs.partitionIndex > -1 {
			f = cs.partitions[cs.partitionIndex]
		}
		cs.RUnlock()

		ok := scanner.Scan()

		if !ok {
			err := scanner.Err()
			fmt.Printf("err: %v\n", err)
			break
		}

		_mode, data := handleMessage(scanner.Text(), conn)

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

	fmt.Println("Client at " + remoteAddr + " disconnected.")

	if mode == INSERT {
		removeDatabaseFiles()
	}
}

func quitConnections() {
	for _, conn := range connections {
		conn.Write([]byte("%quit%\n"))
	}
}

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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func insertData(f *os.File, data []byte) {
	var d map[string]interface{}
	if err := json.Unmarshal(data, &d); err != nil {
		panic(err)
	}

	var lastOffset int64
	cs.RLock()
	l := len(cs.offsets)
	lastOffset = cs.lastOffset
	cs.RUnlock()
	d["id"] = l
	data, _ = json.Marshal(d)

	var length int64 = int64(len(data))

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(length))

	data = append(b, data...)
	n, err := f.WriteAt(data, lastOffset)
	check(err)
	fmt.Printf("wrote %d bytes\n", n)

	cs.Lock()
	cs.offsets = append(cs.offsets, lastOffset)
	cs.partitionRefs = append(cs.partitionRefs, cs.partitionIndex)
	cs.lastOffset = lastOffset + 8 + length
	cs.Unlock()
}

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

func streamRecords(conn net.Conn, data []byte) (err error) {
	query := string(data)
	query, err = expandMacros(query)
	check(err)
	expr, err := Parse(query)
	check(err)
	err = Precompute(expr)
	check(err)

	var n int64 = 0
	var dbStartIndex int64 = -1

	for {
		time.Sleep(10 * time.Millisecond)

		cs.Lock()
		partitionIndex := cs.partitionIndex
		cs.Unlock()

		if partitionIndex == -1 {
			continue
		}

		if dbStartIndex > partitionIndex {
			dbStartIndex = partitionIndex
		} else {
			n = 0
		}

		if dbStartIndex == -1 {
			dbStartIndex = partitionIndex
			if partitionIndex > 1 {
				dbStartIndex -= 1
			}
		}

		cs.Lock()
		f, err := os.Open(cs.partitions[dbStartIndex].Name())
		cs.Unlock()

		if err != nil {
			continue
		}
		f.Seek(n, 0)

		for {
			var b []byte
			b, n, err = readRecord(f, n)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			truth, err := Eval(expr, string(b))
			check(err)

			if truth {
				conn.Write([]byte(fmt.Sprintf("%s\n", b)))
			}
		}

		f.Close()
		dbStartIndex++
	}
}

func retrieveSingle(conn net.Conn, data []byte) (err error) {
	index, _ := strconv.Atoi(string(data))

	cs.RLock()
	l := len(cs.offsets)
	cs.RUnlock()

	if index >= l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", index)))
		return
	}

	var f *os.File
	cs.RLock()
	n := cs.offsets[index]
	i := cs.partitionRefs[index]
	f, err = os.Open(cs.partitions[i].Name())
	cs.RUnlock()
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Record does not exist!\n")))
		return
	}

	f.Seek(n, 0)
	var b []byte
	b, n, err = readRecord(f, n)
	conn.Write([]byte(fmt.Sprintf("%s\n", b)))
	return
}

func validateQuery(conn net.Conn, data []byte) {
	query := string(data)
	_, err := Parse(query)

	if err == nil {
		conn.Write([]byte(fmt.Sprintf("OK\n")))
	} else {
		conn.Write([]byte(fmt.Sprintf("%s\n", err.Error())))
	}
}

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

func setLimit(conn net.Conn, data []byte) {
	value, err := strconv.Atoi(string(data))

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error: While converting the limit to integer: %s\n", err.Error())))
	}

	dbSizeLimit = int64(value) / 2

	conn.Write([]byte(fmt.Sprintf("OK\n")))
}
