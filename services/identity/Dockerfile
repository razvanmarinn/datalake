FROM golang:1.23.4 AS builder
WORKDIR /app
ARG GITHUB_PAT
ENV GOPRIVATE=github.com/razvanmarinn/*
ENV GOPROXY=https://proxy.golang.org,direct

RUN git config --global url."https://${GITHUB_PAT}:x-oauth-basic@github.com/".insteadOf "https://github.com/"
COPY . .


RUN CGO_ENABLED=0 GOOS=linux go build -o identity_service . 
RUN ls -l 

FROM alpine:latest


COPY --from=builder /app/ .


RUN chmod +x identity_service
EXPOSE 8082
CMD ["./identity_service"]
