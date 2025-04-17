package main

import (
	"log"
	"net/http"

	"github.com/razvanmarinn/jwt/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/api_gateway/internal/reverse_proxy"
	pb "github.com/razvanmarinn/datalake/protobuf"
)

func main() {
	identity_service_cnn, err := grpc.Dial("localhost:50056", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	vs := pb.NewVerificationServiceClient(identity_service_cnn)

	r := gin.Default()
	r.Use(middleware.AuthMiddleware())
	r.Any("/ingest/:project/", reverse_proxy.StreamingIngestionProxy(vs, "http://localhost:50057"))
	r.Any("/schema_registry/:project/*path", reverse_proxy.SchemaRegistryProxy("http://localhost:50058"))

	log.Fatal(http.ListenAndServe(":8080", r))
}
