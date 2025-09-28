package handlers

import (
	"context"
	"database/sql"

	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"

	"github.com/razvanmarinn/identity_service/internal/db"
)

type GRPCServer struct {
	pb.UnimplementedVerificationServiceServer
	DB     *sql.DB
	Logger *logging.Logger
}

func (s *GRPCServer) VerifyProjectExistence(ctx context.Context, in *pb.VerifyProjectExistenceRequest) (*pb.VerifyProjectExistenceResponse, error) {
	projectName := in.GetProjectName()
	logger := s.Logger.WithProject(projectName)
	logger.Info("Verifying project existence")
	exists, err := db.CheckProjectExistence(s.DB, projectName)
	if err != nil {
		logger.Error("Failed to check project existence", zap.Error(err))
		return nil, err
	}
	logger.Info("Project existence check completed")
	return &pb.VerifyProjectExistenceResponse{Exists: exists}, nil
}
