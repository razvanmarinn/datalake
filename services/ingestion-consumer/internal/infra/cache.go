package infra

import (
    "fmt"
    "log/slog"
    "sync"

    "github.com/razvanmarinn/ingestion_consumer/internal/batcher"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

// --- SchemaCache ---

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

// --- GRPCConnCache ---

type GRPCConnCache struct {
    mu    sync.RWMutex
    conns map[string]*grpc.ClientConn
}

func NewGRPCConnCache() *GRPCConnCache {
    return &GRPCConnCache{conns: make(map[string]*grpc.ClientConn)}
}

func (cc *GRPCConnCache) Get(addr string) (*grpc.ClientConn, error) {
    // 1. Fast path: Read lock to check if connection exists
    cc.mu.RLock()
    conn, found := cc.conns[addr]
    cc.mu.RUnlock()

    if found {
        // Log on DEBUG or INFO to prove the cache is working!
        slog.Info("gRPC Cache HIT", "address", addr)
        return conn, nil
    }

    // 2. Slow path: Write lock to create connection
    cc.mu.Lock()
    defer cc.mu.Unlock()

    // Double check (another goroutine might have created it while we waited for lock)
    if conn, found = cc.conns[addr]; found {
        slog.Info("gRPC Cache HIT (after lock)", "address", addr)
        return conn, nil
    }

    slog.Info("gRPC Cache MISS - Dialing new connection", "address", addr)

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
    
    count := 0
    for addr, conn := range cc.conns {
        if err := conn.Close(); err != nil {
            slog.Error("failed to close gRPC connection", "address", addr, "error", err)
        }
        count++
    }
    slog.Info("Closed all cached gRPC connections", "count", count)
}