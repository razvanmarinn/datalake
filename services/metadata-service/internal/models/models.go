package models

import (
	"time"

	"github.com/google/uuid"
)

type Block struct {
	BlockID  string
	WorkerID string
	Path     string
	Size     int64
	Format   string
}

type BlockLocation struct {
	BlockID  string
	WorkerID string
	Path     string
}

type CompactionJob struct {
	ID             uuid.UUID
	ProjectID      uuid.UUID
	ProjectName    string
	SchemaID       int
	SchemaName     string
	Status         string
	TargetBlockIDs []string
	OutputFilePath string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type UncompactedFileStat struct {
	ProjectID uuid.UUID
	SchemaID  int
	FileCount int64
}
