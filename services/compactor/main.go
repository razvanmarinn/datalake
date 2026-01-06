package main

import (
	"log"
	"os"
	"path/filepath"

	compactormanager "github.com/razvanmarinn/datalake/compactor/internal/compactor-manager"
)

func main() {
	storageRoot := os.Getenv("STORAGE_ROOT")
	if storageRoot == "" {
		storageRoot = "./data"
	}

	compactor := compactormanager.NewCompactor(compactormanager.Config{
		StorageRoot: storageRoot,
		SchemaAPI:   "http://your-internal-api",
	})

	projects, err := os.ReadDir(storageRoot)
	if err != nil {
		log.Fatalf("Cannot read storage root: %v", err)
	}

	for _, p := range projects {
		if !p.IsDir() {
			continue
		}
		projectPath := filepath.Join(storageRoot, p.Name())

		schemas, err := os.ReadDir(projectPath)
		if err != nil {
			continue
		}

		for _, s := range schemas {
			if !s.IsDir() {
				continue
			}

			log.Printf("Processing Project: %s, Schema: %s", p.Name(), s.Name())

			err := compactor.Compact(p.Name(), s.Name())
			if err != nil {
				log.Printf("Failed to compact %s/%s: %v", p.Name(), s.Name(), err)
			}
		}
	}
}
