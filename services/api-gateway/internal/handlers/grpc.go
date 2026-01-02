package handlers

import (
	"context"
	"log"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"
)

func CheckProjectExists(client pb.MetadataServiceClient, projectName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.VerifyProjectExistenceRequest{
		ProjectName: projectName,
	}

	resp, err := client.VerifyProjectExistence(ctx, req)
	if err != nil {
		log.Printf("gRPC VerifyProjectExistence failed for project %s: %v", projectName, err)
		return false, err
	}

	return resp.Exists, nil
}
