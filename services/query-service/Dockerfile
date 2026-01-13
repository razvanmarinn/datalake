FROM golang:1.24.11 AS builder

WORKDIR /app
ARG GITHUB_PAT
ENV GOPRIVATE=github.com/razvanmarinn/*
ENV GOPROXY=https://proxy.golang.org,direct

RUN git config --global url."https://${GITHUB_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"
COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o query_service . && ls -l


FROM debian:bookworm-slim

WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/ .

RUN chmod +x query_service

EXPOSE 8080
CMD ["./query_service"]
