package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcapi "gpu-telemetry-pipeline/internal/queue/grpc"
	"gpu-telemetry-pipeline/internal/queue/server"
)

func main() {
	addr := getenv("QUEUE_LISTEN_ADDR", ":50051")
	healthAddr := getenv("QUEUE_HEALTH_ADDR", ":8083")
	maxMessages := getenvInt("QUEUE_MAX_MESSAGES", 10000)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var serverOpts []grpc.ServerOption
	certFile := getenv("GRPC_TLS_CERT_FILE", "")
	keyFile := getenv("GRPC_TLS_KEY_FILE", "")
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalf("tls: load cert/key: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		})))
		fmt.Printf("Queue gRPC server TLS enabled (cert: %s)\n", certFile)
	}

	broker := server.NewBroker(maxMessages)
	s := grpc.NewServer(serverOpts...)
	grpcapi.RegisterQueueServiceServer(s, server.NewQueueGRPCServer(broker))

	healthSrv := startHealthServer(healthAddr, broker)

	go func() {
		fmt.Printf("Queue gRPC server listening on %s (max_messages=%d)\n", addr, maxMessages)
		if err := s.Serve(lis); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("shutting down — draining in-flight RPCs...")
	s.GracefulStop()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = healthSrv.Shutdown(shutCtx)
}

func startHealthServer(addr string, broker *server.Broker) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(broker.Stats())
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server error: %v", err)
		}
	}()
	return srv
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
