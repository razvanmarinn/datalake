FROM golang:1.23.4 as tester
WORKDIR /app
COPY . .

CMD ["go", "test", "-v", "./..."]
