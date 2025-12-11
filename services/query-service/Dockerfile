FROM golang:1.24.11 AS builder

WORKDIR /app
ARG GITHUB_PAT
ENV GOPRIVATE=github.com/razvanmarinn/*
ENV GOPROXY=https://proxy.golang.org,direct

RUN git config --global url."https://${GITHUB_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o query_service . && ls -l


FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/ .

RUN chmod +x query_service

EXPOSE 8080
CMD ["./query_service"]
