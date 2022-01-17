ARG ARCH=
ARG GOARCH=
FROM golang:1.16-alpine AS builder
RUN apk add make perl-utils
WORKDIR /tmp
COPY server server
COPY Makefile .
RUN make build GOOS=linux GOARCH=${GOARCH}

FROM ${ARCH}/alpine:3.15
COPY --from=builder ["/tmp/build/basenine_linux_${GOARCH}", "./basenine"]
ENTRYPOINT "./basenine"
