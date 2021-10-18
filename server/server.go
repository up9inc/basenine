package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	jp "github.com/ohler55/ojg/jp"
	oj "github.com/ohler55/ojg/oj"
)

var addr = flag.String("addr", "", "The address to listen to; default is \"\" (all interfaces).")
var port = flag.Int("port", 8000, "The port to listen on; default is 8000.")

type ConnectionMode int

const (
	NONE ConnectionMode = iota
	INSERT
	QUERY
	SINGLE
)

type Commands int

const (
	CMD_INSERT string = "/insert"
	CMD_QUERY  string = "/query"
	CMD_SINGLE string = "/single"
)

const DB_FILE string = "data.bin"

func equx(operand1 string, operand2 string) bool {
	return operand1 == operand2
}

func neqx(operand1 string, operand2 string) bool {
	return operand1 != operand2
}

var operations = map[string]interface{}{
	"==": equx,
	"!=": neqx,
}

var connections []net.Conn

var cs ConcurrentSlice

type ConcurrentSlice struct {
	sync.RWMutex
	lastOffset int64
	offsets    []int64
}

func (cs *ConcurrentSlice) Append(offset int64) {
	cs.Lock()
	defer cs.Unlock()

	cs.offsets = append(cs.offsets, offset)
}

func main() {
	flag.Parse()

	fmt.Println("Starting server...")
	os.Remove(DB_FILE)
	cs = ConcurrentSlice{}

	src := *addr + ":" + strconv.Itoa(*port)
	listener, _ := net.Listen("tcp", src)
	fmt.Printf("Listening on %s.\n", src)

	defer listener.Close()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		quitConnections()
		os.Remove(DB_FILE)
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

func periodicFileSyncer(f *os.File) {
	for {
		time.Sleep(10 * time.Millisecond)
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
	var err error

	defer f.Close()

	for {
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
				f, err = os.OpenFile(DB_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				check(err)
				go periodicFileSyncer(f)
			}
		case INSERT:
			insertData(f, data)
		case QUERY:
			streamRecords(conn, data)
		case SINGLE:
			retrieveSingle(conn, data)
		}
	}

	fmt.Println("Client at " + remoteAddr + " disconnected.")

	if mode == INSERT {
		os.Remove(DB_FILE)
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

	cs.Lock()
	l := len(cs.offsets)
	d["id"] = l
	data, _ = json.Marshal(d)

	var length int64 = int64(len(data))

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(length))
	n, err := f.Write(b)
	check(err)
	fmt.Printf("wrote %d bytes\n", n)

	n, err = f.Write(data)
	check(err)
	fmt.Printf("wrote %d bytes\n", n)

	cs.offsets = append(cs.offsets, cs.lastOffset)
	cs.lastOffset = cs.lastOffset + 8 + length
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
	var path, value, operator string
	var qs []string

	query := string(data)

	if len(strings.TrimSpace(query)) != 0 {
		for key, _ := range operations {
			if strings.Contains(query, key) {
				operator = key
				qs = strings.Split(query, key)
			}
		}

		if operator == "" {
			err = errors.New("Unidentified operation.")
		}

		path = strings.TrimSpace(qs[0])
		value = strings.TrimSpace(qs[1])
		value = strings.Trim(value, "\"")

		fmt.Printf("path: %v\n", path)
		fmt.Printf("value: %v\n", value)
		fmt.Printf("operator: %v\n", operator)
	}

	var n int64 = 0
	var i int = 0

	for {
		time.Sleep(10 * time.Millisecond)
		f, err := os.Open(DB_FILE)
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

			var truth bool
			if path != "" {
				truth, err = JsonPath(path, string(b), value, operator)
				check(err)
			} else {
				truth = true
			}

			if truth {
				conn.Write([]byte(fmt.Sprintf("%s\n", b)))
			}
		}

		f.Close()
		i++
	}
}

func JsonPath(path string, text string, ref string, operator string) (truth bool, err error) {
	obj, err := oj.ParseString(text)
	if err != nil {
		return
	}

	x, err := jp.ParseString(path)
	if err != nil {
		return
	}
	result := x.Get(obj)

	var exists bool
	var value string

	if len(result) < 1 {
		exists = false
	} else {
		exists = true
		switch result[0].(type) {
		case string:
			value = result[0].(string)
		case int64:
			value = strconv.FormatInt(result[0].(int64), 10)
		case float64:
			value = strconv.FormatFloat(result[0].(float64), 'g', 6, 64)
		case bool:
			value = strconv.FormatBool(result[0].(bool))
		case nil:
			value = "null"
		default:
			exists = false
		}
	}

	if exists {
		truth = operations[operator].(func(string, string) bool)(value, ref)
	} else if operator == "!=" {
		truth = true
	}

	return
}

func retrieveSingle(conn net.Conn, data []byte) (err error) {
	index, _ := strconv.Atoi(string(data))

	cs.RLock()
	l := len(cs.offsets)
	cs.RUnlock()

	if index-1 > l {
		conn.Write([]byte(fmt.Sprintf("Index out of range: %d\n", index)))
		return
	}

	cs.RLock()
	n := cs.offsets[index]
	cs.RUnlock()

	f, err := os.Open(DB_FILE)
	check(err)
	f.Seek(n, 0)
	var b []byte
	b, n, err = readRecord(f, n)
	conn.Write([]byte(fmt.Sprintf("%s\n", b)))
	return
}
