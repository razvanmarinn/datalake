package main

import (
	"log"

	"github.com/razvanmarinn/datalake/pkg/metrics"
	"github.com/razvanmarinn/schema-registry/internal/db"
	"github.com/razvanmarinn/schema-registry/internal/handlers"
)

func main() {
	database, err := db.Connect_to_db()

	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	serviceMetrics := metrics.NewServiceMetrics("schema-registry")

	r := handlers.SetupRouter(database)

	metrics.SetupMetricsEndpoint(r)

	r.Use(serviceMetrics.PrometheusMiddleware())

	r.Run(":8080")
}
