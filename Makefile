default:
	cd server/ && go build -gcflags="-e" -o ../build/basenine *.go

test:
	cd server/ && go test *.go -v
