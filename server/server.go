// Copyright 2022 UP9. All rights reserved.
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
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	basenine "github.com/up9inc/basenine/server/lib"
	"github.com/up9inc/basenine/server/lib/storages"
)

var addr = flag.String("addr", "", "The address to listen to; default is \"\" (all interfaces).")
var port = flag.Int("port", 9099, "The port to listen on.")
var debug = flag.Bool("debug", false, "Enable debug logs.")
var version = flag.Bool("version", false, "Print version and exit.")
var persistent = flag.Bool("persistent", false, "Enable persistent mode. Dumps core on exit.")
var storageDriver = flag.String("storage", "native", "The storage driver for saving the records; default is \"native\" (.db files in pwd).")
var storageArgs = flag.String("storage-args", "", "Arguments for the storage driver.")

var storage basenine.Storage

// Slice that stores the TCP connections
var connections []net.Conn

func main() {
	// Parse the command-line arguments.
	flag.Parse()

	// Print version and exit.
	if *version {
		fmt.Printf("%s\n", basenine.VERSION)
		// 0: process exited normally
		os.Exit(0)
	}

	log.Printf("Basenine Community (Version: %s)\n", basenine.VERSION)

	switch *storageDriver {
	case "native":
		storage = storages.NewNativeStorage(*persistent)
		log.Printf("Using native storage driver.\n")
	default:
		log.Panicf("Unknown storage driver: %s", *storageDriver)
	}

	// Start listenning to given address and port.
	src := *addr + ":" + strconv.Itoa(*port)
	listener, err := net.Listen("tcp", src)
	basenine.Check(err)
	log.Printf("Listening on %s\n", src)

	defer listener.Close()

	// Make a channel to gracefully close the TCP connections.
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Handle the channel.
	go func() {
		sig := <-c
		quitConnections()
		err = storage.HandleExit(sig.(syscall.Signal), *persistent)
		basenine.Check(err)
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

// handleConnection handles a TCP connection
func handleConnection(conn net.Conn) {
	// Append connection into a global slice
	connections = append(connections, conn)

	// Log the connection
	remoteAddr := conn.RemoteAddr().String()
	if *debug {
		log.Println("Client connected from " + remoteAddr)
	}

	// Create a scanner
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)

	// Prevent buffer overflows
	scanner.Buffer(buf, 209715200)

	// Set connection mode to NONE
	var mode basenine.ConnectionMode = basenine.NONE

	// Arguments for the QUERY command (query, fetch, timeoutMs)
	var queryArgs []string

	// Arguments for the SINGLE command (index, query)
	var singleArgs []string

	// Arguments for the FETCH command (leftOff, direction, query, limit)
	var fetchArgs []string

	var err error
	for {
		// Scan the input
		ok := scanner.Scan()

		if !ok {
			if *debug {
				err = scanner.Err()
				log.Printf("Scanning error: %v\n", err)
			}
			break
		}

		// Handle the message
		_mode, data := handleMessage(scanner.Text(), conn)

		// Set the connection mode
		switch mode {
		case basenine.NONE:
			mode = _mode
			switch mode {
			case basenine.FLUSH:
				err = storage.Flush()
				basenine.SendErr(conn, err)
				if err == nil {
					basenine.SendOK(conn)
				}
			case basenine.RESET:
				err = storage.Reset()
				basenine.SendErr(conn, err)
				if err == nil {
					basenine.SendOK(conn)
				}
			}
		case basenine.INSERT:
			_, err = storage.InsertData(data)
		case basenine.INSERTION_FILTER:
			err = storage.SetInsertionFilter(conn, data)
			basenine.SendErr(conn, err)
		case basenine.QUERY:
			if len(queryArgs) < 3 {
				queryArgs = append(queryArgs, string(data))
			}
			if len(queryArgs) == 3 {
				err = storage.StreamRecords(conn, queryArgs[0], queryArgs[1], queryArgs[2])
			}
		case basenine.SINGLE:
			if len(singleArgs) < 2 {
				singleArgs = append(singleArgs, string(data))
			}
			if len(singleArgs) == 2 {
				err = storage.RetrieveSingle(conn, singleArgs[0], singleArgs[1])
			}
		case basenine.FETCH:
			if len(fetchArgs) < 4 {
				fetchArgs = append(fetchArgs, string(data))
			}
			if len(fetchArgs) == 4 {
				err = storage.Fetch(conn, fetchArgs[0], fetchArgs[1], fetchArgs[2], fetchArgs[3])
			}
		case basenine.VALIDATE:
			err = storage.ValidateQuery(conn, data)
		case basenine.MACRO:
			err = storage.ApplyMacro(conn, data)
			basenine.SendErr(conn, err)
		case basenine.LIMIT:
			err = storage.SetLimit(conn, data)
			basenine.SendErr(conn, err)
		case basenine.FLUSH:
			err = storage.Flush()
			basenine.SendErr(conn, err)
			if err == nil {
				basenine.SendOK(conn)
			}
		case basenine.RESET:
			err = storage.Reset()
			basenine.SendErr(conn, err)
			if err == nil {
				basenine.SendOK(conn)
			}
		}

		if err != nil {
			break
		}
	}

	// Close the file descriptor for this TCP connection
	conn.Close()
	// Log the disconnect
	if *debug {
		log.Println("Client at " + remoteAddr + " disconnected.")
	}
}

// quitConnections quits all of the active TCP connections. It's only called
// in case of an interruption.
func quitConnections() {
	for _, conn := range connections {
		basenine.SendClose(conn)
	}
}

// handleMessage handles given message string of a TCP connection and returns a
// ConnectionMode to set the mode of the that TCP connection.
func handleMessage(message string, conn net.Conn) (mode basenine.ConnectionMode, data []byte) {
	if *debug {
		log.Println("> " + message)
	}

	if len(message) > 0 && message[0] == '/' {
		switch {
		case message == basenine.CMD_INSERT:
			mode = basenine.INSERT
			return

		case strings.HasPrefix(message, basenine.CMD_INSERTION_FILTER):
			mode = basenine.INSERTION_FILTER

		case strings.HasPrefix(message, basenine.CMD_QUERY):
			mode = basenine.QUERY

		case message == basenine.CMD_SINGLE:
			mode = basenine.SINGLE

		case message == basenine.CMD_FETCH:
			mode = basenine.FETCH

		case strings.HasPrefix(message, basenine.CMD_VALIDATE):
			mode = basenine.VALIDATE

		case strings.HasPrefix(message, basenine.CMD_MACRO):
			mode = basenine.MACRO

		case strings.HasPrefix(message, basenine.CMD_LIMIT):
			mode = basenine.LIMIT

		case message == basenine.CMD_FLUSH:
			mode = basenine.FLUSH

		case message == basenine.CMD_RESET:
			mode = basenine.RESET

		default:
			conn.Write([]byte("Unrecognized command.\n"))
		}
	} else {
		data = []byte(message)
	}

	return
}
