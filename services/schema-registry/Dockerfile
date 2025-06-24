FROM golang:1.23.4 AS builder
WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o schema_registry . 
RUN ls -l 

FROM alpine:latest


COPY --from=builder /app/ .


RUN chmod +x schema_registry
EXPOSE 8080
CMD ["./schema_registry"]
