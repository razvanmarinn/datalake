package main

import (
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/datalake/pkg/metrics"
	"github.com/razvanmarinn/schema-registry/internal/db"
	"github.com/razvanmarinn/schema-registry/internal/handlers"
	"go.uber.org/zap"
)

func main() {
	logger := logging.NewDefaultLogger("schema-registry")
	serviceMetrics := metrics.NewServiceMetrics("schema-registry")
	database, err := db.Connect_to_db(logger)

	if err != nil {
		logger.Error("Failed to connect to database: %v", zap.Error(err))
	}
	defer database.Close()


	r := handlers.SetupRouter(database, logger)

	metrics.SetupMetricsEndpoint(r)

	r.Use(serviceMetrics.PrometheusMiddleware())

	r.Run(":8080")
}
