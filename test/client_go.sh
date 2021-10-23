#!/bin/bash

./basenine & \
PID=$! && \
cd client/go/ && go test *.go -v
EXIT_CODE=$?

kill -2 $PID

exit $EXIT_CODE
