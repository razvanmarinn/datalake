package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/razvanmarinn/ingestion_consumer/internal/config"
	"github.com/razvanmarinn/ingestion_consumer/internal/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config load failed", "error", err)
		os.Exit(1)
	}

	app, err := service.NewApp(cfg, logger)
	if err != nil {
		logger.Error("app init failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Run the App in a BACKGROUND goroutine with a RETRY LOOP
	go func() {
		for {
			// Check if we are supposed to stop (shutdown signal received)
			if ctx.Err() != nil {
				return
			}

			logger.Info("starting consumer loop")

			// This blocks until the consumer fails or stops
			app.Run(ctx)

			// If we are here, the consumer stopped unexpectedly.
			// Do NOT cancel(). Instead, log and retry.
			logger.Error("consumer loop stopped unexpectedly. Restarting in 5 seconds...")

			select {
			case <-time.After(5 * time.Second):
				// Continue loop and restart Run()
			case <-ctx.Done():
				return // Exit goroutine if main context is cancelled
			}
		}
	}()

	// 2. Setup signal capturing
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 3. BLOCK here until a signal is received
	select {
	case sig := <-sigChan:
		logger.Info("shutdown signal received", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down")
	}

	// 4. Perform graceful shutdown
	cancel()       // Signal cancellation to any remaining contexts
	app.Shutdown() // Cleanup resources
	logger.Info("shutdown complete")
}
