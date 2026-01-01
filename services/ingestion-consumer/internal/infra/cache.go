package infra

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SchemaCache
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*batcher.Schema
}

func NewSchemaCache() *SchemaCache {
	return &SchemaCache{cache: make(map[string]*batcher.Schema)}
}

func (sc *SchemaCache) Get(key string) (*batcher.Schema, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	schema, found := sc.cache[key]
	return schema, found
}

func (sc *SchemaCache) Set(key string, schema *batcher.Schema) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.cache[key] = schema
}

// GRPCConnCache
type GRPCConnCache struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCConnCache() *GRPCConnCache {
	return &GRPCConnCache{conns: make(map[string]*grpc.ClientConn)}
}

func (cc *GRPCConnCache) Get(addr string) (*grpc.ClientConn, error) {
	cc.mu.RLock()
	conn, found := cc.conns[addr]
	cc.mu.RUnlock()
	if found {
		return conn, nil
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	// Double check
	if conn, found = cc.conns[addr]; found {
		return conn, nil
	}

	// 64MB limit
	opts := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(64 * 1024 * 1024),
		grpc.MaxCallSendMsgSize(64 * 1024 * 1024),
	}

	newConn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(opts...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	cc.conns[addr] = newConn
	return newConn, nil
}

func (cc *GRPCConnCache) CloseAll() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for addr, conn := range cc.conns {
		if err := conn.Close(); err != nil {
			slog.Error("failed to close gRPC connection", "address", addr, "error", err)
		}
	}
}
