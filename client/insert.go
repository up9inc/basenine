package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"time"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 9099, "The port to connect to; defaults to 9099.")

type Car struct {
	Id    int      `json:"id"`
	Model string   `json:"model"`
	Brand CarBrand `json:"brand"`
	Year  int      `json:"year"`
}

type CarBrand struct {
	Name string `json:"name"`
}

type School struct {
	Id         int          `json:"id"`
	Name       string       `json:"name"`
	League     SchoolLeague `json:"league"`
	Address    string       `json:"address"`
	Enrollment int          `json:"enrollment"`
	Score      float64      `json:"score"`
	Year       int          `json:"year"`
}

type SchoolLeague struct {
	Name string `json:"name"`
}

func main() {
	flag.Parse()

	a := &Car{
		Model: "Camaro",
		Brand: CarBrand{
			Name: "Chevrolet",
		},
		Year: 2021,
	}

	b := &School{
		Name: "Harvard",
		League: SchoolLeague{
			Name: "Ivy",
		},
		Address:    "Massachusetts",
		Enrollment: 5000,
		Score:      4.8,
		Year:       1636,
	}

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

	go readConnection(conn)

	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	conn.Write([]byte("/insert\n"))

	var data []byte
	for i := 0; i < 10000000; i++ {
		fmt.Printf("%d\n", i)
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if i%2 == 1 {
			b.Id = i - 1
			data, _ = json.Marshal(b)
		} else {
			a.Id = i - 1
			data, _ = json.Marshal(a)
		}

		conn.Write(data)

		conn.Write([]byte("\n"))

		// Comment out the line below to really stress things out
		time.Sleep(1 * time.Millisecond)
	}

	time.Sleep(1 * time.Hour)
}

func readConnection(conn net.Conn) {
	for {
		scanner := bufio.NewScanner(conn)

		for {
			ok := scanner.Scan()
			text := scanner.Text()

			command := handleCommands(text)
			if !command {
				fmt.Printf("\b\b** %s\n> ", text)
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
