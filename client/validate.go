package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 9099, "The port to connect to; defaults to 9099.")
var query = flag.String("query", "", "Query for filtering the records.")

func main() {
	flag.Parse()

	dest := *host + ":" + strconv.Itoa(*port)
	fmt.Printf("Connecting to %s...\n", dest)

	conn, err := net.Dial("tcp", dest)

	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	var wg sync.WaitGroup
	go readConnection(&wg, conn)
	wg.Add(1)

	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	conn.Write([]byte("/validate\n"))

	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	conn.Write([]byte(fmt.Sprintf("%s\n", *query)))

	wg.Wait()
}

func readConnection(wg *sync.WaitGroup, conn net.Conn) {
	defer wg.Done()
	for {
		scanner := bufio.NewScanner(conn)

		for {
			ok := scanner.Scan()
			text := scanner.Text()

			command := handleCommands(text)
			if !command {
				fmt.Printf("\b\b** %s\n> ", text)
				os.Exit(0)
			}

			if !ok {
				fmt.Println("Reached EOF on server connection.")
				break
			}
		}
	}
}

func handleCommands(text string) bool {
	r, err := regexp.Compile("^%.*%$")
	if err == nil {
		if r.MatchString(text) {

			switch {
			case text == "%quit%":
				fmt.Println("\b\bServer is leaving. Hanging up.")
				os.Exit(0)
			}

			return true
		}
	}

	return false
}