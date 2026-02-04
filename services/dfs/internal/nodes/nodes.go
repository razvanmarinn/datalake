package nodes

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

var lock sync.Mutex

type InodeType int

const (
	FileType InodeType = iota
	DirType
)

type Node interface {
	Start()
	Stop()
	HealthCheck() bool
}

type Inode struct {
	ID        string
	Name      string
	Path      string
	OwnerID   string
	Type      InodeType
	ProjectID string
	Size      int64
	Blocks    []uuid.UUID
	Children  []string
}
type BlockMetadata struct {
	BlockID     uuid.UUID   `json:"blockId"`
	Size        int64       `json:"size"`
	Checksum    uint32      `json:"checksum"`
	Version     int64       `json:"version"`
	PrimaryNode string      `json:"primaryNode"`
	LeaseExpiry time.Time   `json:"leaseExpiry"`
	Replicas    []uuid.UUID `json:"replicas"`
}
