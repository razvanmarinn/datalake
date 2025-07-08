FROM golang:1.23.4-alpine AS tester

WORKDIR /app

RUN apk add --no-cache bash

COPY . .


CMD ["go", "test", "-v", "./..."]
