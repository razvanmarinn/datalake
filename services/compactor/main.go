package main

import (
	"log"
	"os"
	"path/filepath"

	compactormanager "github.com/razvanmarinn/datalake/compactor/internal/compactor-manager"
)

func main() {
	storageRoot := os.Getenv("STORAGE_ROOT") // This is /data (pointing to /datalake/worker_data)
	if storageRoot == "" {
		storageRoot = "./data"
	}

	compactor := compactormanager.NewCompactor(compactormanager.Config{
		StorageRoot: storageRoot,
		SchemaAPI:   os.Getenv("SCHEMA_API_URL"), // e.g. http://metadata-service:8080
	})

	workers, err := os.ReadDir(storageRoot)
	if err != nil {
		log.Fatalf("Cannot read storage root: %v", err)
	}

	for _, w := range workers {
		if !w.IsDir() {
			continue
		}
		workerPath := filepath.Join(storageRoot, w.Name())

		projects, err := os.ReadDir(workerPath)
		if err != nil {
			log.Printf("Skipping worker dir %s: %v", w.Name(), err)
			continue
		}

		for _, p := range projects {
			if !p.IsDir() {
				continue
			}
			projectPath := filepath.Join(workerPath, p.Name())

			schemas, err := os.ReadDir(projectPath)
			if err != nil {
				continue
			}

			for _, s := range schemas {
				if !s.IsDir() || s.Name() == "compacted" {
					continue
				}

				log.Printf("Found Target -> Worker: %s, Project: %s, Schema: %s", w.Name(), p.Name(), s.Name())

				relativeProjectPath := filepath.Join(w.Name(), p.Name())

				err := compactor.Compact(relativeProjectPath, s.Name())
				if err != nil {
					log.Printf("❌ Failed to compact %s/%s in %s: %v", p.Name(), s.Name(), w.Name(), err)
				}
			}
		}
	}
	log.Println("✅ Compaction cycle finished.")
}