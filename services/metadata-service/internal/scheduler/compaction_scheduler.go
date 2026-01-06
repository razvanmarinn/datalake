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
		if stat.FileCount >= COMPACTION_THRESHOLD {
			s.Logger.Info("Found project/schema meeting compaction threshold",
				zap.String("project_id", stat.ProjectID.String()),
				zap.Int("schema_id", stat.SchemaID),
				zap.Int64("file_count", stat.FileCount))

			blockIDs, err := db.GetUncompactedBlockIDs(s.DB, stat.ProjectID, stat.SchemaID, COMPACTION_THRESHOLD)
			if err != nil {
				s.Logger.Error("Failed to get uncompacted block IDs", zap.Error(err))
				continue
			}

			if len(blockIDs) > 0 {
				_, err := db.CreateCompactionJob(s.DB, stat.ProjectID, stat.SchemaID, blockIDs)
				if err != nil {
					s.Logger.Error("Failed to create compaction job", zap.Error(err))
					continue
				}
				s.Logger.Info("Created compaction job",
					zap.String("project_id", stat.ProjectID.String()),
					zap.Int("schema_id", stat.SchemaID),
					zap.Int("block_count", len(blockIDs)))
			}
		}
	}
}
