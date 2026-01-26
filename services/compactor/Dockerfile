FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY services/compactor/go.mod services/compactor/go.sum ./services/compactor/

COPY go.mod* go.sum* go.work* ./

COPY pkg/ ./pkg/
COPY protobuf/ ./protobuf/
COPY services/compactor/ ./services/compactor/

RUN cd services/compactor && CGO_ENABLED=0 GOOS=linux go build -o /app/compactor main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/compactor .
CMD ["./compactor"]
