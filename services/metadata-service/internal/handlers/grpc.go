package handlers

import (
	"context"
	"database/sql"

	"github.com/razvanmarinn/datalake/pkg/logging"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"go.uber.org/zap"

	"github.com/razvanmarinn/metadata-service/internal/db"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCServer struct {
	pb.UnimplementedMetadataServiceServer
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

func (s *GRPCServer) GetCompactionJob(ctx context.Context, req *pb.GetCompactionJobRequest) (*pb.GetCompactionJobResponse, error) {
	s.Logger.Info("Received GetCompactionJob request", zap.String("project_id", req.GetProjectId()), zap.String("schema_name", req.GetSchemaName()))

	// TODO: Implement actual logic to fetch a compaction job from the database
	// For now, return a dummy job
	return &pb.GetCompactionJobResponse{
		Job: &pb.CompactionJob{
			Id:             "dummy-job-id-123",
			ProjectId:      "project_x",
			SchemaName:     "schema_a",
			Status:         "pending",
			TargetBlockIds: []string{"block1", "block2", "block3"},
			CreatedAt:      timestamppb.Now(),
			UpdatedAt:      timestamppb.Now(),
		},
	}, nil
}

func (s *GRPCServer) UpdateCompactionJobStatus(ctx context.Context, req *pb.UpdateCompactionJobStatusRequest) (*pb.SimpleResponse, error) {
	s.Logger.Info("Received UpdateCompactionJobStatus request", zap.String("job_id", req.GetJobId()), zap.String("status", req.GetStatus()))

	// TODO: Implement actual logic to update job status in the database
	// Also handle output_file_path and compacted_block_ids
	return &pb.SimpleResponse{
		Success: true,
		Message: "Compaction job status updated successfully",
	}, nil
}

func (s *GRPCServer) RegisterBlock(ctx context.Context, req *pb.RegisterBlockRequest) (*pb.SimpleResponse, error) {
	s.Logger.Info("Received RegisterBlock request", zap.String("block_id", req.GetBlockId()), zap.String("project_id", req.GetProjectId()))

	// TODO: Implement actual logic to store block metadata in the database
	// Use s.DB to interact with the data_files table
	return &pb.SimpleResponse{
		Success: true,
		Message: "Block registered successfully",
	}, nil
}

func (s *GRPCServer) GetBlockLocations(ctx context.Context, req *pb.GetBlockLocationsRequest) (*pb.GetBlockLocationsResponse, error) {
	s.Logger.Info("Received GetBlockLocations request", zap.Strings("block_ids", req.GetBlockIds()))

	// TODO: Implement actual logic to fetch block locations from the database
	// For now, return dummy locations
	locations := make([]*pb.BlockLocation, len(req.GetBlockIds()))
	for i, blockID := range req.GetBlockIds() {
		locations[i] = &pb.BlockLocation{
			BlockId:  blockID,
			WorkerId: "dummy-worker-id-1",
			Path:     "/data/" + blockID + ".bin",
		}
	}

	return &pb.GetBlockLocationsResponse{
		Locations: locations,
	}, nil
}
