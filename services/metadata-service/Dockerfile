FROM golang:1.25.0 AS builder
WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o metadata-service .

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/metadata-service .
COPY --from=builder /app/migrations ./migrations

RUN chmod +x metadata-service
EXPOSE 8081
CMD ["./metadata-service"]
