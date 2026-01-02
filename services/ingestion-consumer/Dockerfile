# Builder stage
FROM golang:1.23-bullseye AS builder

RUN apt-get update && apt-get install -y librdkafka-dev ca-certificates

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=arm64

RUN go build -o ingestion-consumer .

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y librdkafka1 ca-certificates

WORKDIR /app
COPY --from=builder /app/ingestion-consumer .

CMD ["./ingestion-consumer"]
