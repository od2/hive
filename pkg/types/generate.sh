#!/bin/sh

protoc -I=. \
  --go_out=plugins:. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  ./*.proto
