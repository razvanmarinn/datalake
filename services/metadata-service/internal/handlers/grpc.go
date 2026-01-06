package handlers

import (
    "context"
    "database/sql"
    "fmt"

    "github.com/google/uuid"
    "github.com/razvanmarinn/datalake/pkg/logging"
    pb "github.com/razvanmarinn/datalake/protobuf"
    "go.uber.org/zap"

    "github.com/razvanmarinn/metadata-service/internal/db"
    "github.com/razvanmarinn/metadata-service/internal/models"
    "google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCServer struct {
    pb.UnimplementedMetadataServiceServer
    DB     *sql.DB
    Logger *logging.Logger
}

func (s *GRPCServer) GetCompactionJob(ctx context.Context, req *pb.GetCompactionJobRequest) (*pb.GetCompactionJobResponse, error) {
    s.Logger.Info("Received GetCompactionJob request", zap.String("project_id", req.GetProjectId()), zap.String("schema_name", req.GetSchemaName()))

    // 1. Parse Project UUID
    projectID, err := uuid.Parse(req.GetProjectId())
    if err != nil {
        return nil, fmt.Errorf("invalid project ID format: %w", err)
    }

    projectName, err := db.GetProjectNameByID(s.DB, projectID) // You might need to add this helper to db.go
    if err != nil {
         return nil, fmt.Errorf("project not found: %w", err)
    }

    schemaID, err := db.GetSchemaID(s.DB, projectName, req.GetSchemaName())
    if err != nil {
        return nil, fmt.Errorf("schema not found: %w", err)
    }

    // 3. Get the Job
    job, err := db.GetPendingCompactionJob(s.DB, projectID, schemaID)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch compaction job: %w", err)
    }
    if job == nil {
        // No job found
        return &pb.GetCompactionJobResponse{}, nil
    }

    // 4. Map DB Model to Protobuf
    return &pb.GetCompactionJobResponse{
        Job: &pb.CompactionJob{
            Id:             job.ID.String(),
            ProjectId:      job.ProjectID.String(),
            SchemaName:     req.GetSchemaName(),
            Status:         job.Status,
            TargetBlockIds: job.TargetBlockIDs,
            CreatedAt:      timestamppb.New(job.CreatedAt),
            UpdatedAt:      timestamppb.New(job.UpdatedAt),
        },
    }, nil
}

func (s *GRPCServer) UpdateCompactionJobStatus(ctx context.Context, req *pb.UpdateCompactionJobStatusRequest) (*pb.SimpleResponse, error) {
    s.Logger.Info("Received UpdateCompactionJobStatus request", zap.String("job_id", req.GetJobId()), zap.String("status", req.GetStatus()))

    jobID, err := uuid.Parse(req.GetJobId())
    if err != nil {
        return &pb.SimpleResponse{Success: false, Message: "Invalid Job ID"}, err
    }

    // Call the real DB function
    err = db.UpdateCompactionJobStatus(s.DB, jobID, req.GetStatus(), req.GetOutputFilePath(), req.GetCompactedBlockIds())
    if err != nil {
        s.Logger.Error("Failed to update compaction job", zap.Error(err))
        return &pb.SimpleResponse{Success: false, Message: "Failed to update database"}, err
    }

    return &pb.SimpleResponse{
        Success: true,
        Message: "Compaction job status updated successfully",
    }, nil
}

func (s *GRPCServer) RegisterBlock(ctx context.Context, req *pb.RegisterBlockRequest) (*pb.SimpleResponse, error) {
    s.Logger.Info("Received RegisterBlock request", zap.String("block_id", req.GetBlockId()))

    // 1. Parse IDs
    projectID, err := uuid.Parse(req.GetProjectId())
    if err != nil {
        return nil, fmt.Errorf("invalid project ID: %w", err)
    }
    
    // Helper needed: Get ProjectName from ID to lookup SchemaID
    projectName, err := db.GetProjectNameByID(s.DB, projectID) 
    if err != nil { return nil, err }

    schemaID, err := db.GetSchemaID(s.DB, projectName, req.GetSchemaName())
    if err != nil {
        return nil, fmt.Errorf("schema lookup failed: %w", err)
    }

    // 2. Create Block Model
    block := models.Block{
        BlockID:  req.GetBlockId(),
        WorkerID: req.GetWorkerId(),
        Path:     req.GetPath(),
        Size:     req.GetSize(),
        Format:   "parquet", // Or pass this in request if dynamic
    }

    // 3. Store in DB
    err = db.RegisterBlock(s.DB, projectID, schemaID, block)
    if err != nil {
        s.Logger.Error("Failed to register block", zap.Error(err))
        return &pb.SimpleResponse{Success: false, Message: "Database error"}, err
    }

    return &pb.SimpleResponse{
        Success: true,
        Message: "Block registered successfully",
    }, nil
}

func (s *GRPCServer) GetBlockLocations(ctx context.Context, req *pb.GetBlockLocationsRequest) (*pb.GetBlockLocationsResponse, error) {
    // 1. Get from DB
    dbLocations, err := db.GetBlockLocations(s.DB, req.GetBlockIds())
    if err != nil {
        return nil, fmt.Errorf("failed to fetch locations: %w", err)
    }

    // 2. Map to Protobuf
    pbLocations := make([]*pb.MetadataBlockLocation, len(dbLocations))
    for i, loc := range dbLocations {
        pbLocations[i] = &pb.MetadataBlockLocation{
            BlockId:  loc.BlockID,
            WorkerId: loc.WorkerID,
            Path:     loc.Path,
        }
    }

    return &pb.GetBlockLocationsResponse{
        Locations: pbLocations,
    }, nil
}