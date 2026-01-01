FROM golang:1.25.0 AS builder
WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o metadata-service . 
RUN ls -l 

FROM alpine:latest


COPY --from=builder /app/ .
COPY --from=builder /app/sql ./sql

RUN chmod +x metadata-service
EXPOSE 8080
CMD ["./metadata-service"]
