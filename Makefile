default:
	cd server/ && go build -gcflags="-e" -o ../build/basenine *.go
