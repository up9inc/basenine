basenine:
	cd server/ && go build -gcflags="-e" -o ../basenine *.go

test: basenine test-client-go
	cd server/ && go test *.go -v

test-client-go:
	test/client_go.sh
