package main

import (
	"log"

	"github.com/razvanmarinn/schema-registry/internal/handlers"

	"github.com/razvanmarinn/schema-registry/internal/db"
)

// test
func main() {
	database, err := db.Connect_to_db()

	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	r := handlers.SetupRouter(database)
	r.Run(":8081")
}
