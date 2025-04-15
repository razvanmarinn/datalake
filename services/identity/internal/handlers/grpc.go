package handlers

import (
	"context"
	"database/sql"
	"log"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/razvanmarinn/identity_service/internal/db"
)

type GRPCServer struct {
	pb.UnimplementedVerificationServiceServer
	DB *sql.DB
}

func (s *GRPCServer) VerifyProjectExistence(ctx context.Context, in *pb.VerifyProjectExistenceRequest) (*pb.VerifyProjectExistenceResponse, error) {
	projectName := in.GetProjectName()
	log.Printf("Verifying existence of project: %s", projectName)
	exists, err := db.CheckProjectExistence(s.DB, projectName)
	if err != nil {
		return nil, err
	}
	log.Printf("Project %s exists: %v", projectName, exists)
	return &pb.VerifyProjectExistenceResponse{Exists: exists}, nil
}
