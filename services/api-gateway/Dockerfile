FROM golang:1.23.4 AS builder

WORKDIR /app
ARG GITHUB_PAT
ENV GOPRIVATE=github.com/razvanmarinn/*
ENV GOPROXY=https://proxy.golang.org,direct

RUN git config --global url."https://${GITHUB_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o api_gateway . && ls -l


FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/ .

RUN chmod +x api_gateway

EXPOSE 8080
CMD ["./api_gateway"]
