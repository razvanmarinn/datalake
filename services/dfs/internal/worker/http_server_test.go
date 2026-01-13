package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHTTPServer_SecurityAndAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dfs-worker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	validBlockID := "block-123"
	validFilename := validBlockID + ".bin"
	validContent := []byte("hello world")
	if err := os.WriteFile(filepath.Join(tmpDir, validFilename), validContent, 0644); err != nil {
		t.Fatal(err)
	}

	server := NewHTTPServer(tmpDir, 8080)

	tests := []struct {
		name           string
		requestPath    string
		expectedStatus int
		description    string
	}{
		{
			name:           "Valid Block Access",
			requestPath:    "/blocks/" + validBlockID, // URL uses ID, server adds .bin
			expectedStatus: http.StatusOK,
			description:    "Should allow reading a valid block in the storage dir",
		},
		{
			name:           "Missing Block",
			requestPath:    "/blocks/missing-block",
			expectedStatus: http.StatusNotFound,
			description:    "Should return 404 for non-existent blocks",
		},
		{
			name:           "Path Traversal Simple",
			requestPath:    "/blocks/../secret.txt",
			expectedStatus: http.StatusForbidden,
			description:    "Should block attempts to go up directory",
		},
		{
			name:           "Path Traversal Complex",
			requestPath:    "/blocks/%2e%2e%2fsecret.txt",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:           "Root Access Attempt",
			requestPath:    "/blocks//etc/passwd",
			expectedStatus: http.StatusForbidden,
			description:    "Should block attempts to access absolute paths",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", tt.requestPath, nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			server.handleDownload(rr, req)

			if status := rr.Code; status != tt.expectedStatus {
				t.Errorf("handler returned wrong status code for %s: got %v want %v",
					tt.name, status, tt.expectedStatus)
			}
		})
	}
}