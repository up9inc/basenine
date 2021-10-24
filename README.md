# Basenine

<p align="center">
    <a href="https://github.com/up9inc/basenine/releases/latest">
        <img alt="GitHub Latest Release" src="https://img.shields.io/github/v/release/up9inc/basenine?logo=GitHub&style=flat-square">
    </a>
    <a href="https://github.com/up9inc/basenine/blob/master/LICENSE">
        <img alt="GitHub License" src="https://img.shields.io/github/license/up9inc/basenine?logo=GitHub&style=flat-square">
    </a>
    <a href="https://github.com/up9inc/basenine/actions?query=workflow%3ATest">
        <img alt="GitHub Workflow Tests" src="https://img.shields.io/github/workflow/status/up9inc/basenine/Test?logo=GitHub&label=tests&style=flat-square">
    </a>
    <a href="https://codecov.io/gh/up9inc/basenine">
        <img alt="Code Coverage (Codecov)" src="https://img.shields.io/codecov/c/github/up9inc/basenine?logo=Codecov&style=flat-square">
    </a>
</p>

Schema-free, document-oriented streaming database that optimized for monitoring network traffic in real-time.

### Featured Aspects

- Has the fastest possible write speed.
- Has a read speed that scales linearly.
- Schema-free.
- Only allows create and read.
- Accepts JSON as the record format.
- Let's you query based on JSONPath.
- Has a rich filtering sytnax for querying.
- Defines a TCP-based protocol.
- Has long lasting TCP connections.
- Watches the database and streams back the new records.

## Server

Run the server:

`make && ./basenine -port 9099`

### Protocol

The database server has six modes:

- **Insert mode** provies a long lasting TCP connection to insert data into the `data_*.bin` binary files on server's directory.
A client can elevate itself to insert mode by sending `/insert` command.

- **Query mode** let's you filter the records in the database based on a [filtering syntax named BFL](https://github.com/up9inc/basenine/wiki/BFL-Syntax-Reference).
Query mode streams the results to the client and is able to keep up where it left off even if the database have millions of records.
The TCP connection in this mode is long lasting as well. The filter cannot be changed without establishing a new connnection.

- **Single mode** is a short lasting TCP connection that returns a single record from the database based on the provided index value.

- **Validate mode** checks the query against syntax errors. Returns the error if it's syntactically invalid otherwise returns `OK`.

- **Macro mode** let's you define a macro for the query language like `http~proto.name == "http"`.

- **Limit mode** allows you to set a hard-limit for the database size in bytes like `100000000` (100MB).
The disk usage ranges between `50000000` (50MB) and `100000000` (100MB).
So the actual effective limit is the half of this value.

### Query

Querying achieved through a filter syntax named **Basenine Filter Language (BFL)**. It enables the user to query the traffic logs efficiently and precisely.

```python
http and request.method == "GET" and request.path != "/example" and (request.query.a > 42 or request.headers["x"] == "y")
```

Please [see the syntax reference](https://github.com/up9inc/basenine/wiki/BFL-Syntax-Reference) for more info.

## Client

### Go

Insert:

```go
// Establish a new connection to a Basenine server at localhost:9099
c, err := NewConnection("localhost", "9099")
if err != nil {
    panic(err)
}

// Elevate to INSERT mode
c.InsertMode()

// There can be many Send and SendText calls
c.SendText(`{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2019}`)
c.Send([]byte(`{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2020}`))
c.SendText(`{"brand":{"name":"Chevrolet"},"model":"Camaro","year":2021}`)

// Close
c.Close()
```

Single:

```go
// Retrieve the record with ID equals to 42
data, err := Single("localhost", "9099", 42)
if err != nil {
    panic(err)
}
```

Query:

```go
// Establish a new connection to a Basenine server at localhost:9099
c, err := NewConnection("localhost", "9099")
if err != nil {
    panic(err)
}

// Make a []byte channel to recieve the data
data := make(chan []byte)

// Define a function to handle the stream
handleDataChannel := func(wg *sync.WaitGroup, c *Connection, data chan []byte) {
    defer wg.Done()
    for {
        bytes := <-data

        // Do something with bytes

        c.Close()
    }
}

var wg sync.WaitGroup
go handleDataChannel(&wg, c, data)
wg.Add(1)

c.Query(`brand.name == "Chevrolet"`, data)

wg.Wait()
```

Validate:

```go
err := Validate("localhost", "9099", `brand.name == "Chevrolet"`)
if err != nil {
    // err should be nil, otherwise a connection error or a syntax error
}
```

Macro:

```go
// Define a macro `chevy` expands into `brand.name == "Chevrolet"`
err := Macro("localhost", "9099", "chevy", `brand.name == "Chevrolet"`)
if err != nil {
    // err can only be a connection error
}
```

Limit:

```go
// Set the database size limit to 100MB
err := Limit("localhost", "9099", 100000000)
if err != nil {
    // err can only be a connection error
}
```
