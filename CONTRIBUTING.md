# Contributing to Basenine

## Code of Conduct

This project and everyone participating in it is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code. Please report unacceptable behavior to [info@up9.com](mailto:info@up9.com).

## Build

Run `make`, the executable named `basenine` should be placed in the project root.

To cross-compile run `make build-all`. It places a bunch of executables prefixed `basenine_` into `build/` directory.

## Make Changes

### Server

The `server/` directory contains the code of Basenine.

`server/parser.go` contains the lexer and the grammar definition to parse [BFL](https://github.com/up9inc/basenine/wiki/BFL-Syntax-Reference).

`server/precompute.go` contains the code for doing compile-time evaluations on a given BFL query.

`server/eval.go` contains the code for evaluating boolean truthiness of a given BFL query and the corresponding JSON document.

`server/macro.go` contains the codef for definining and expanding macros on BFL queries.

`server/server.go` is the main file that implements the TCP server's itself. It's also responsible to partition the database
and binding the TCP-based protocol with functionalities provided by the files above.

### Client

`client/` directory contains the client implementation for various languages. These are the official clients that are
developed and maintained by the Basenine's itself.

## Test

Run `make test`, it will run the tests for both the server and the clients. It should exit with `0`.
Also check the coverage info printed into `stdout`.

## Submit

[Open a pull request](https://github.com/up9inc/basenine/compare) from your fork to `main` branch of Basenine.
