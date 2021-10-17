# Basenine

Schema-free, document-oriented streaming database that optimized for monitoring network traffic in real-time.

### Featured Aspects

- Has a fast write speed.
- Has a fast read speed.
- Schema-free.
- Only allows create and read.
- Accepts JSON as the record format.
- Let's you query based on JSONPath.
- Defines a TCP-based text protocol.
- Has long lasting TCP connections.
- Watches the database file and streams back the new records.

## Server

Run the server:

`make && build/basenine -port 8000`

The database server has three modes:

```go
const (
	NONE ConnectionMode = iota
	INSERT
	QUERY
	SINGLE
)
```

**Insert mode** provies a long lasting TCP connection to insert data into the `data.bin` binary file on server's directory.
A client can elevate itself to insert mode by sending `/insert` command. `data.bin` file is removed upon closing an insert mode connection.

**Query mode** let's you filter the records in the `data.bin` file based on a primitive syntax like `brand.name == \"Chevrolet\"` which supports two operators: `==` and `!=`.
Query mode streams the results to the client and is able to keep up where it left off even if the database have millions of records. The TCP connection in this mode is long lasting too. The filter cannot be changed without establishing a new connnection.

**Single mode** is a short lasting TCP connection that returns a single record from `data.bin` based on the provided index value.

## Client

Clients vary according to the three connection modes.

> Note: Don't close the **insert mode** client while trying other clients. Closing **insert mode** client removes `data.bin` file.

### Insert

Run the client:

`go run client/insert.go -host localhost -port 8000`

The client example for the **insert mode** defines these four structs:

```go
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
```

Then establishes as an insert mode connection to the server with:

```go
conn.Write([]byte("/insert\n"))
```

Then up to 10 million records, quickly inserts into the database:

```go
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
```

### Query

Run the client:

`go run client/query.go -host localhost -port 8000 -query "brand.name == \"Chevrolet\""`

It establishes as a **query mode** connection to the server with:

```go
conn.Write([]byte("/query\n"))
```

Then constantly prints the server's response:

```go
fmt.Printf("\b\b** %s\n> ", text)
```

```text
...
** {"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}
** {"id":114907,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}
** {"id":114909,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}
** {"id":114911,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}
** {"id":114913,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}
> 
```

Try to stop <kbd>CTRL</kbd>+<kbd>C</kbd> and re-run the client after a while. It will be able to catch the stream in a glimpse of an eye.

Try to negate the query and run:

`go run client/query.go -host localhost -port 8000 -query "brand.name != \"Chevrolet\""`

```text
...
** {"id":4376,"name":"Harvard","league":{"name":"Ivy"},"address":"Massachusetts","enrollment":5000,"score":4.8,"year":1636}
** {"id":4378,"name":"Harvard","league":{"name":"Ivy"},"address":"Massachusetts","enrollment":5000,"score":4.8,"year":1636}
** {"id":4380,"name":"Harvard","league":{"name":"Ivy"},"address":"Massachusetts","enrollment":5000,"score":4.8,"year":1636}
** {"id":4382,"name":"Harvard","league":{"name":"Ivy"},"address":"Massachusetts","enrollment":5000,"score":4.8,"year":1636}
** {"id":4384,"name":"Harvard","league":{"name":"Ivy"},"address":"Massachusetts","enrollment":5000,"score":4.8,"year":1636}
> 
```

This time, the records of other struct will be printed.

Also try the queries below:

`go run client/query.go -host localhost -port 8000`

`go run client/query.go -host localhost -port 8000 -query ""`

`go run client/query.go -host localhost -port 8000 -query "id == 3"`

`go run client/query.go -host localhost -port 8000 -query "model == \"Camaro\""`

`go run client/query.go -host localhost -port 8000 -query "year == 1636"`

`go run client/query.go -host localhost -port 8000 -query "year == 2021"`

`go run client/query.go -host localhost -port 8000 -query "league.name == \"Ivy\""`

`go run client/query.go -host localhost -port 8000 -query "league.name != \"Ivy\""`

`go run client/query.go -host localhost -port 8000 -query "score == 4.8"`

> Note: Closing 10 million records gap took 53 seconds in our tests. Which is equal to `1006M	data.bin` file. (~1GB)

### Single

Run the client:

`go run client/single.go -host localhost -port 8000 -index 100`

It establishes as a **single mode** connection to the server with:

```go
conn.Write([]byte("/single\n"))
```

and retrieves a single record based on the index provided with `-index`:

```text
Connecting to localhost:8000...
** {"id":100,"name":"Harvard","league":{"name":"Ivy"},"address":"Massachusetts","enrollment":5000,"score":4.8,"year":1636}
>
```

## TODOS

- Make the protocol binary to improve the transmission speed.
- Add lexer, parser, AST to really implement the query syntax.
- Improve the querying speed by keep tracking checksums of strings.
- Write a client library for Go and Python.
- Add tests.
- Improve logging, introduce debug level.
- Implement mechanisms to reduce data redundancy.
