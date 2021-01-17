#syntax=docker/dockerfile:1.2

FROM golang:1.15-alpine AS builder
WORKDIR /app
COPY . .
RUN \
   --mount=type=cache,target=/go/pkg \
   --mount=type=cache,target=/root/.cache/go-build \
   go build -o hive ./cmd

FROM alpine
COPY --from=builder /app/hive /usr/local/bin/
CMD ["/usr/local/bin/hive"]
LABEL org.opencontainers.image.source="https://github.com/od2/hive"
