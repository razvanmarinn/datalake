# Stage 1: Build
FROM golang:1.22 AS builder
WORKDIR /app

COPY . .


WORKDIR /app/internal/api
RUN CGO_ENABLED=0 GOOS=linux go build -o api . 
RUN ls -l 

FROM alpine:latest


COPY --from=builder /app/internal/api/ .


RUN chmod +x api

CMD ["./api"]
