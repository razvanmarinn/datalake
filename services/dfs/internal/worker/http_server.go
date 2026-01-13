package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type HTTPServer struct {
	storageDir string
	port       int
	server     *http.Server
}

func NewHTTPServer(storageDir string, port int) *HTTPServer {
	return &HTTPServer{
		storageDir: storageDir,
		port:       port,
	}
}

func (s *HTTPServer) Start() {
	mux := http.NewServeMux()
	mux.HandleFunc("/blocks/", s.handleDownload)

	addr := fmt.Sprintf(":%d", s.port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Minute, 
	}

	go func() {
		log.Printf("HTTP Server listening on %s", addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP Server failed: %v", err)
		}
	}()
}

// Stop gracefully shuts down the server
func (s *HTTPServer) Stop() {
	if s.server != nil {
		s.server.Close()
	}
}

func (s *HTTPServer) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract block ID from path (e.g. /blocks/abc-123 -> abc-123)
	blockID := strings.TrimPrefix(r.URL.Path, "/blocks/")
	if blockID == "" {
		http.Error(w, "Block ID required", http.StatusBadRequest)
		return
	}

	// SECURITY: Prevent Path Traversal
	// 1. Clean the path
	cleanPath := filepath.Clean(blockID)
	// 2. Reject if it contains ".." or starts with "/" or "\"
	if strings.Contains(cleanPath, "..") || strings.HasPrefix(cleanPath, "/") || strings.HasPrefix(cleanPath, "\") {
		log.Printf("Security Alert: Path traversal attempt detected: %s", blockID)
		http.Error(w, "Invalid block ID", http.StatusForbidden)
		return
	}

	// Construct full path
	// The WorkerNode stores files with a .bin extension (see worker.go)
	fullPath := filepath.Join(s.storageDir, cleanPath+".bin")

	// SECURITY: Double check that the final path is still within storageDir
	absStorageDir, _ := filepath.Abs(s.storageDir)
	absFullPath, _ := filepath.Abs(fullPath)
	if !strings.HasPrefix(absFullPath, absStorageDir) {
		log.Printf("Security Alert: Resolved path outside storage dir: %s", fullPath)
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	// Check if file exists
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		http.Error(w, "Block not found", http.StatusNotFound)
		return
	}

	log.Printf("Serving block via HTTP: %s.bin", cleanPath)
	http.ServeFile(w, r, fullPath)
}
