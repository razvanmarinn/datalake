package scheduler

import (
	"database/sql"
	"time"

	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/metadata-service/internal/db"
	"go.uber.org/zap"
)

const (
	COMPACTION_THRESHOLD = 3
	MAX_FILES_PER_JOB    = 50
	FETCH_LIMIT          = 5000
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

		candidates, err := db.GetUncompactedBlockIDs(s.DB, stat.ProjectID, stat.SchemaID, FETCH_LIMIT)
		if err != nil {
			s.Logger.Error("Failed to get uncompacted block IDs", zap.Error(err))
			continue
		}

		var freeBlockIDs []string
		for _, blockID := range candidates {
			if !pendingBlocksMap[blockID] {
				freeBlockIDs = append(freeBlockIDs, blockID)
			}
		}

		jobsCreated := 0
		for i := 0; i < len(freeBlockIDs); i += MAX_FILES_PER_JOB {
			end := i + MAX_FILES_PER_JOB
			if end > len(freeBlockIDs) {
				end = len(freeBlockIDs)
			}

			batch := freeBlockIDs[i:end]

			if len(batch) >= COMPACTION_THRESHOLD {
				_, err := db.CreateCompactionJob(s.DB, stat.ProjectID, stat.SchemaID, batch)
				if err != nil {
					s.Logger.Error("Failed to create compaction job batch", zap.Error(err))
					continue // Try the next batch even if this one failed
				}
				jobsCreated++
			}
		}

		if jobsCreated > 0 {
			s.Logger.Info("Scheduled compaction jobs",
				zap.String("project", stat.ProjectID.String()),
				zap.Int("schema", stat.SchemaID),
				zap.Int("jobs_created", jobsCreated),
				zap.Int("total_files_scheduled", len(freeBlockIDs)))
		}
	}
}
