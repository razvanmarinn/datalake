package main

import (
	"context"
	"log"
	"os"
	"time"

	compactormanager "github.com/razvanmarinn/datalake/compactor/internal/compactor-manager"
	pb "github.com/razvanmarinn/datalake/protobuf"
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
		metadataServiceAddr = "metadata-service:50051"
	}

	conn, err := grpc.Dial(metadataServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to metadata service: %v", err)
	}
	defer conn.Close()

	metadataClient := pb.NewMetadataServiceClient(conn)

	compactor := compactormanager.NewCompactor(compactormanager.Config{
		StorageRoot:    storageRoot,
		SchemaAPI:      os.Getenv("SCHEMA_API_URL"), // e.g. http://metadata-service:8080
		MetadataClient: metadataClient,
	})

	log.Println("🔎 Fetching compaction job...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := metadataClient.GetCompactionJob(ctx, &pb.GetCompactionJobRequest{})
	if err != nil {
		log.Fatalf("Failed to get compaction job: %v", err)
	}

	job := resp.GetJob()
	if job == nil || job.Id == "" {
		log.Println("No pending compaction jobs found.")
		return
	}

	log.Printf("Starting compaction job for Project: %s, Schema: %s", job.ProjectId, job.SchemaName)
	err = compactor.Compact(ctx, job)
	if err != nil {
		log.Printf("❌ Failed to compact %s/%s: %v", job.ProjectId, job.SchemaName, err)
	}

	log.Println("✅ Compaction cycle finished.")
}

