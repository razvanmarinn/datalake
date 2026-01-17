package handlers

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/razvanmarinn/datalake/pkg/logging"
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
	"go.uber.org/zap"

	"github.com/razvanmarinn/metadata-service/internal/db"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCServer struct {
	catalogv1.UnimplementedCatalogServiceServer
	DB     *sql.DB
	Logger *logging.Logger
}

func (s *GRPCServer) GetProject(ctx context.Context, req *catalogv1.GetProjectRequest) (*catalogv1.GetProjectResponse, error) {

	projectName := req.GetProjectName()

	s.Logger.Info("Received GetProject request", zap.String("project_name", projectName))

	projectId, err := db.GetProjectUUIDByProjectName(s.DB, projectName)
	project, err := db.GetProjectByID(s.DB, projectId)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("project not found")
		}
		s.Logger.Error("Failed to fetch project", zap.Error(err))
		return nil, fmt.Errorf("internal database error")
	}

	return &catalogv1.GetProjectResponse{
		ProjectId: projectId.String(),
		OwnerId:   project.Owner,
		CreatedAt: timestamppb.New(project.CreatedAt),
	}, nil
}

func (s *GRPCServer) PollCompactionJobs(ctx context.Context, req *catalogv1.PollCompactionJobsRequest) (*catalogv1.PollCompactionJobsResponse, error) {
	s.Logger.Info("Polling for compaction jobs")

	dbJobs, err := db.PollPendingCompactionJobs(s.DB)
	if err != nil {
		s.Logger.Error("Failed to poll compaction jobs", zap.Error(err))
		return nil, fmt.Errorf("failed to fetch jobs: %w", err)
	}

	grpcJobs := make([]*catalogv1.PollCompactionJob, 0, len(dbJobs))

	for _, job := range dbJobs {

		grpcJobs = append(grpcJobs, &catalogv1.PollCompactionJob{
			JobId:       job.ID.String(),
			ProjectId:   job.ProjectID.String(),
			ProjectName: job.ProjectName,
			SchemaName:  job.SchemaName,
			TargetFiles: job.TargetBlockIDs,
			TargetPaths: job.TargetPaths,
		})
	}

	return &catalogv1.PollCompactionJobsResponse{
		Jobs: grpcJobs,
	}, nil
}

func (s *GRPCServer) UpdateJobStatus(ctx context.Context, req *catalogv1.UpdateJobStatusRequest) (*catalogv1.UpdateJobStatusResponse, error) {
	s.Logger.Info("Received UpdateJobStatus request",
		zap.String("job_id", req.GetJobId()),
		zap.String("status", req.GetStatus()),
	)

	jobID, err := uuid.Parse(req.GetJobId())
	if err != nil {
		return &catalogv1.UpdateJobStatusResponse{Success: false}, fmt.Errorf("invalid job ID: %w", err)
	}

	err = db.UpdateCompactionJobStatus(s.DB, jobID, req.GetStatus(), req.GetResultFilePath())
	if err != nil {
		s.Logger.Error("Failed to update job status", zap.Error(err))
		return &catalogv1.UpdateJobStatusResponse{Success: false}, err
	}

	return &catalogv1.UpdateJobStatusResponse{
		Success: true,
	}, nil
}

func (s *GRPCServer) RegisterDataFile(ctx context.Context, req *catalogv1.RegisterDataFileRequest) (*catalogv1.RegisterDataFileResponse, error) {
	s.Logger.Info("Received RegisterDataFile request",
		zap.String("project_id", req.GetProjectId()),
		zap.String("schema_name", req.GetSchemaName()),
		zap.String("block_id", req.GetBlockId()),
	)

	projectID, err := uuid.Parse(req.GetProjectId())
	if err != nil {
		return &catalogv1.RegisterDataFileResponse{Success: false}, fmt.Errorf("invalid project ID: %w", err)
	}

	err = db.RegisterDataFile(s.DB, projectID.String(), req.GetSchemaName(), req.GetBlockId(), req.GetWorkerId(), req.GetFilePath(), req.GetFileSize(), req.GetFileFormat())
	if err != nil {
		s.Logger.Error("Failed to register data file", zap.Error(err))
		return &catalogv1.RegisterDataFileResponse{Success: false}, err
	}

	return &catalogv1.RegisterDataFileResponse{
		Success: true,
	}, nil
}
