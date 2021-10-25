FROM golang:1.16-alpine AS builder
RUN apk add make
WORKDIR /tmp
COPY server server
COPY Makefile .
RUN make

FROM alpine:3.13.5
COPY --from=builder ["/tmp/basenine", "."]
ENTRYPOINT "./basenine"
