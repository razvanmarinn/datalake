package nodes

import (
	"context"
	"log"
	"os"
	"strings"
	"time"
)

type IntegrityChecker struct {
	worker       *WorkerNode
	checkInterval time.Duration
	stopChan     chan struct{}
}

func NewIntegrityChecker(worker *WorkerNode, checkInterval time.Duration) *IntegrityChecker {
	return &IntegrityChecker{
		worker:       worker,
		checkInterval: checkInterval,
		stopChan:     make(chan struct{}),
	}
}

func (ic *IntegrityChecker) Start() {
	go ic.runPeriodicChecks()
	log.Printf("🔍 Integrity Checker started (interval: %v)", ic.checkInterval)
}

func (ic *IntegrityChecker) Stop() {
	close(ic.stopChan)
	log.Println("🛑 Integrity Checker stopped")
}

func (ic *IntegrityChecker) runPeriodicChecks() {
	ticker := time.NewTicker(ic.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ic.checkAllBlocks()
		case <-ic.stopChan:
			return
		}
	}
}

func (ic *IntegrityChecker) checkAllBlocks() {
	log.Println("🔍 Starting periodic integrity check...")

	files, err := os.ReadDir(ic.worker.StorageDir)
	if err != nil {
		log.Printf("Error reading storage directory: %v", err)
		return
	}

	checkedCount := 0
	corruptedCount := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if strings.HasSuffix(file.Name(), ".bin") {
			blockID := strings.TrimSuffix(file.Name(), ".bin")

			if err := ic.worker.verifyBlockIntegrity(blockID); err != nil {
				log.Printf("❌ CORRUPTION DETECTED: Block %s failed integrity check: %v", blockID, err)
				corruptedCount++
				ic.handleCorruptedBlock(blockID)
			} else {
				checkedCount++
			}
		}
	}

	if corruptedCount > 0 {
		log.Printf("⚠️ Integrity check complete: %d blocks checked, %d CORRUPTED", checkedCount, corruptedCount)
	} else {
		log.Printf("✅ Integrity check complete: %d blocks verified, all healthy", checkedCount)
	}
}

func (ic *IntegrityChecker) handleCorruptedBlock(blockID string) {
	log.Printf("🚨 Handling corrupted block %s - marking for replication", blockID)
}

func (ic *IntegrityChecker) CheckBlock(ctx context.Context, blockID string) error {
	return ic.worker.verifyBlockIntegrity(blockID)
}
