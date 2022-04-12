SUFFIX=$(GOOS)_$(GOARCH)

.PHONY: build

basenine:
	cd server/ && go build -gcflags="-e" -ldflags="-s -w" -o ../basenine *.go

dev:
	cd server/ && go build -gcflags="-e" -o ../basenine *.go

install: basenine
	mv basenine /usr/local/bin/

clean:
	rm basenine || true

test: clean basenine test-server test-client-go coverage

test-server:
	cd server/ && go test *.go -v -race -covermode=atomic -coverprofile=coverage.out
	cd server/lib && go test *.go -v -race -covermode=atomic -coverprofile=coverage.out
	cd server/lib/connectors && go test *.go -v -race -covermode=atomic -coverprofile=coverage.out

test-client-go:
	test/client_go.sh

build:
	cd server/ && CGO_ENABLED=0 go build -gcflags="-e" -ldflags="-extldflags=-static -s -w" -o ../build/basenine_$(SUFFIX) *.go && \
	cd ../build/ && shasum -a 256 basenine_${SUFFIX} > basenine_${SUFFIX}.sha256

build-all:
	rm -rf build/ && \
	$(MAKE) build GOOS=linux GOARCH=amd64
	$(MAKE) build GOOS=linux GOARCH=arm64
	$(MAKE) build GOOS=linux GOARCH=386
	$(MAKE) build GOOS=darwin GOARCH=amd64
	$(MAKE) build GOOS=darwin GOARCH=arm64

coverage:
	cp server/coverage.out coverage.out && sed 1,1d server/lib/coverage.out >> coverage.out && sed 1,1d client/go/coverage.out >> coverage.out

install-init-systemd:
	cp scripts/init/systemd/basenine.service /etc/systemd/system/ && \
	systemctl daemon-reload && \
	systemctl restart basenine && \
	systemctl status basenine

install-init-rc:
	cp scripts/init/rc/basenine /etc/init.d/basenine && \
	rc-update add basenine boot && \
	rc-service basenine start && \
	rc-service basenine status
