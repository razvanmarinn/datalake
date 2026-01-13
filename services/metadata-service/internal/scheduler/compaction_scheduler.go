package scheduler

import (
	"database/sql"
	"time"

	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/metadata-service/internal/db"
	"go.uber.org/zap"
)

const (
	COMPACTION_THRESHOLD = 3 // Number of uncompacted files to trigger a job
)

type CompactionScheduler struct {
	DB     *sql.DB
	Logger *logging.Logger
}

func NewCompactionScheduler(db *sql.DB, logger *logging.Logger) *CompactionScheduler {
	return &CompactionScheduler{
		DB:     db,
		Logger: logger,
	}
}

func (s *CompactionScheduler) Start() {
	s.Logger.Info("Starting compaction scheduler")
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.Logger.Info("Running compaction job check")
				s.createCompactionJobs()
			}
		}
	}()
}
func (s *CompactionScheduler) createCompactionJobs() {
	stats, err := db.GetUncompactedFileStats(s.DB)
	if err != nil {
		s.Logger.Error("Failed to get uncompacted file stats", zap.Error(err))
		return
	}

	for _, stat := range stats {
		if stat.FileCount < COMPACTION_THRESHOLD {
			continue
		}

		pendingBlocksMap, err := db.GetBlockIDsInPendingCompactionJobs(s.DB, stat.ProjectID, stat.SchemaID)
		if err != nil {
			s.Logger.Error("Failed to fetch pending jobs", zap.String("project", stat.ProjectID.String()), zap.Error(err))
			continue
		}

		candidates, err := db.GetUncompactedBlockIDs(s.DB, stat.ProjectID, stat.SchemaID, COMPACTION_THRESHOLD+20)
		if err != nil {
			s.Logger.Error("Failed to get uncompacted block IDs", zap.Error(err))
			continue
		}

		var validBlockIDs []string
		for _, blockID := range candidates {
			if !pendingBlocksMap[blockID] { // Only checks against THIS project's pending list
				validBlockIDs = append(validBlockIDs, blockID)
			}
		}
		if len(validBlockIDs) >= COMPACTION_THRESHOLD {
			if len(validBlockIDs) > COMPACTION_THRESHOLD {
				validBlockIDs = validBlockIDs[:COMPACTION_THRESHOLD]
			}

			_, err := db.CreateCompactionJob(s.DB, stat.ProjectID, stat.SchemaID, validBlockIDs)
			if err != nil {
				s.Logger.Error("Failed to create compaction job", zap.Error(err))
				continue
			}

			s.Logger.Info("Created compaction job",
				zap.String("project", stat.ProjectID.String()),
				zap.Int("schema", stat.SchemaID))
		}
	}
}
