module github.com/razvanmarinn/ingestion_consumer

go 1.23.4

require (
	github.com/google/uuid v1.6.0
	github.com/razvanmarinn/datalake v0.0.0-20250322185346-7ce66f242c6e
	github.com/segmentio/kafka-go v0.4.47
	google.golang.org/grpc v1.71.0
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace github.com/razvanmarinn/datalake => /Users/marinrazvan/Developer/datalake
