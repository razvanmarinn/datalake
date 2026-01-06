FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o compactor main.go

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=builder /app/compactor .
RUN chmod +x ./compactor
CMD ["./compactor"]