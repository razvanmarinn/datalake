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

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			logger.Info("starting consumer loop")

			app.Run(ctx)

			logger.Error("consumer loop stopped unexpectedly. Restarting in 5 seconds...")

			select {
			case <-time.After(5 * time.Second):

			case <-ctx.Done():
				return
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info("shutdown signal received", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down")
	}

	cancel()
	app.Shutdown()
	logger.Info("shutdown complete")
}
