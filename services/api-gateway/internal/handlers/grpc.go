package handlers

import catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"

// import (
// 	"context"
// 	"log"
// 	"time"
// 	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
// )

// func CheckProjectExists(client catalogv1.CatalogServiceClient, projectName string) (bool, error) {
// 	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	// defer cancel()

// 	// req := &catalogv1.VerifyProjectExistenceRequest{
// 	// 	ProjectName: projectName,
// 	// }

// 	// resp, err := client.VerifyProjectExistence(ctx, req)
// 	// if err != nil {
// 	// 	log.Printf("gRPC VerifyProjectExistence failed for project %s: %v", projectName, err)
// 	// 	return false, err
// 	// }

// 	return resp.Exists, nil
// }

func CheckProjectExists(client catalogv1.CatalogServiceClient, projectName string) (bool, error) {
	return true, nil
}
