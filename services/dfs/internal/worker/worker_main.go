package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
	"github.com/razvanmarinn/dfs/internal/nodes"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultPort     = 50051
	defaultHTTPPort = 8080
	storageDir      = "/data"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting worker node...")

	portStr := os.Getenv("GRPC_PORT")
	port := defaultPort

	if portStr != "" {
		cleanPort := strings.TrimPrefix(portStr, ":")
		p, err := strconv.Atoi(cleanPort)
		if err != nil {
			log.Fatalf("Invalid GRPC_PORT: %v", err)
		}
		port = p
	}

	httpPortStr := os.Getenv("HTTP_PORT")
	httpPort := defaultHTTPPort
	if httpPortStr != "" {
		cleanPort := strings.TrimPrefix(httpPortStr, ":")
		p, err := strconv.Atoi(cleanPort)
		if err != nil {
			log.Fatalf("Invalid HTTP_PORT: %v", err)
		}
		httpPort = p
	}

	listenAddr := ":" + strconv.Itoa(port)

	state := nodes.NewWorkerNodeState()

	if err := state.LoadStateFromFile(); err != nil {
		log.Printf("No existing state found or failed to load, starting fresh: %v", err)
	}

	worker := nodes.NewWorkerNode(storageDir, port)

	if state.ID != "" {
		log.Printf("Restoring previous Worker ID: %s", state.ID)
		worker.ID = state.ID
	} else {
		log.Printf("Initialized new Worker ID: %s", worker.ID)
	}

	worker.Start()

	integrityChecker := nodes.NewIntegrityChecker(worker, 1*time.Hour)
	integrityChecker.Start()

	httpServer := NewHTTPServer(storageDir, httpPort)
	httpServer.Start()

	go func() {
		metricsPort := httpPort + 1
		http.Handle("/metrics", promhttp.Handler())
		metricsAddr := ":" + strconv.Itoa(metricsPort)
		log.Printf("Metrics server listening on %s", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Worker node listening on %s", listenAddr)

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(64 * 1024 * 1024),
		grpc.MaxSendMsgSize(64 * 1024 * 1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 20 * time.Second,
		}),
		grpc.Creds(insecure.NewCredentials()),
	}

	grpcServer := grpc.NewServer(opts...)

	healthServer := health.NewServer()

	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	log.Println("Health check service registered successfully")

	datanodev1.RegisterDataNodeServiceServer(grpcServer, worker)

	go func() {
		log.Println("Starting gRPC server...")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	log.Println("gRPC server started successfully")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		sig := <-sigs
		log.Printf("Received signal: %v", sig)
		log.Println("Shutting down worker node...")

		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		if err := state.UpdateState(worker); err != nil {
			log.Printf("failed to update state: %v", err)
		}

		if err := state.SaveState(); err != nil {
			log.Printf("failed to save state: %v", err)
		}

		grpcServer.GracefulStop()
		integrityChecker.Stop()
		worker.Stop()
		httpServer.Stop()
		log.Println("Worker node stopped")
	}()

	log.Println("Worker node is running. Press Ctrl+C to stop.")
	wg.Wait()
	log.Println("Main function exiting !")
}
