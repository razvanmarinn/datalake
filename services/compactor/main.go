package main

import (
	"context"
	"log"
	"os"
	"time"

	compactormanager "github.com/razvanmarinn/datalake/compactor/internal/compactor-manager"
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	storageRoot := os.Getenv("STORAGE_ROOT")
	if storageRoot == "" {
		storageRoot = "./data"
	}

	metadataServiceAddr := os.Getenv("METADATA_SERVICE_ADDR")
	if metadataServiceAddr == "" {
		metadataServiceAddr = "metadata-service:50055"
	}

	conn, err := grpc.Dial(metadataServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to metadata service: %v", err)
	}
	defer conn.Close()

	metadataClient := catalogv1.NewCatalogServiceClient(conn)
	masterClient := coordinatorv1.NewCoordinatorServiceClient(conn)
	compactor := compactormanager.NewCompactor(compactormanager.Config{
		StorageRoot:   storageRoot,
		SchemaAPI:     os.Getenv("SCHEMA_API_URL"), // e.g. http://metadata-service:8080
		CatalogClient: metadataClient,
		MasterClient:  masterClient,
	})

	log.Println("🔎 Fetching compaction job...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := metadataClient.PollCompactionJobs(ctx, &catalogv1.PollCompactionJobsRequest{})
	if err != nil {
		log.Fatalf("Failed to get compaction job: %v", err)
	}
	jobId := resp.GetJobId()
	schemaName := resp.GetSchemaName()
	projectId := resp.GetProjectId()
	targetFiles := resp.GetTargetFiles()
	err = compactor.Compact(ctx, jobId, projectId, schemaName, targetFiles)
	if err != nil {
		log.Printf("❌ Failed to compact %s/%s: %v", projectId, schemaName, err)
	}

	log.Println("✅ Compaction cycle finished.")
}
