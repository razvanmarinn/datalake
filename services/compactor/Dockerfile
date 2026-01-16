FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ ./pkg/

COPY protobuf/ ./protobuf/
COPY services/compactor/ ./services/compactor/

RUN CGO_ENABLED=0 GOOS=linux go build -o compactor ./services/compactor/main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/compactor .

CMD ["./compactor"]
