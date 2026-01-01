FROM golang:1.23.4 AS builder
WORKDIR /app

COPY . .

ARG GITHUB_PAT

RUN git config --global url."https://${GITHUB_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

RUN CGO_ENABLED=0 GOOS=linux go build -o ingestion-consumer .
RUN ls -l

FROM alpine:latest

COPY --from=builder /app/ .

RUN chmod +x ingestion-consumer
EXPOSE 8080
CMD ["./ingestion-consumer"]