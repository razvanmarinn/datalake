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

	// --- 1. Connect to Metadata Service ---
	metadataServiceAddr := os.Getenv("METADATA_SERVICE_ADDR")
	if metadataServiceAddr == "" {
		metadataServiceAddr = "metadata-service:50055"
	}

	metaConn, err := grpc.Dial(metadataServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to metadata service: %v", err)
	}
	defer metaConn.Close()

	metadataClient := catalogv1.NewCatalogServiceClient(metaConn)

	masterServiceAddr := os.Getenv("MASTER_SERVICE_ADDR")
	if masterServiceAddr == "" {
		masterServiceAddr = "master.datalake.svc.cluster.local:50055"
	}
	masterConn, err := grpc.Dial(masterServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to master service: %v", err)
	}
	defer masterConn.Close()

	masterClient := coordinatorv1.NewCoordinatorServiceClient(masterConn)

	compactor := compactormanager.NewCompactor(compactormanager.Config{
		StorageRoot:   storageRoot,
		SchemaAPI:     "http://metadata-service.datalake.svc.cluster.local:8081",
		CatalogClient: metadataClient,
		MasterClient:  masterClient, // Now using the correct client
	})

	log.Println("🔎 Fetching compaction job...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := metadataClient.PollCompactionJobs(ctx, &catalogv1.PollCompactionJobsRequest{})
	if err != nil {
		log.Fatalf("Failed to get compaction job: %v", err)
	}

	// Extract details
	jobId := resp.GetJobId()
	schemaName := resp.GetSchemaName()
	projectId := resp.GetProjectId()
	projectName := resp.GetProjectName()
	targetFiles := resp.GetTargetFiles()
	targetPaths := resp.GetTargetPaths()
	respGetProj, _ := metadataClient.GetProject(ctx, &catalogv1.GetProjectRequest{ProjectName: projectName})
	ownerId := respGetProj.OwnerId
	log.Printf("⚙️ Starting compaction for Job: %s (Project: %s, Schema: %s)", jobId, projectName, schemaName)

	err = compactor.Compact(ctx, jobId, projectId, projectName, schemaName, targetFiles, targetPaths, ownerId)
	if err != nil {
		log.Printf("❌ Failed to compact %s/%s: %v", projectId, schemaName, err)
	} else {
		log.Println("✅ Compaction cycle finished.")
	}
}
