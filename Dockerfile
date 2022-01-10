FROM golang:1.16-alpine AS builder
RUN apk add make perl-utils
WORKDIR /tmp
COPY server server
COPY Makefile .
RUN make build GOOS=linux GOARCH=amd64

FROM alpine:3.15
COPY --from=builder ["/tmp/build/basenine_linux_amd64", "./basenine"]
ENTRYPOINT "./basenine"
