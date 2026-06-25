package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcapi "gpu-telemetry-pipeline/internal/queue/grpc"
	"gpu-telemetry-pipeline/internal/queue/server"
)

func main() {
	addr := os.Getenv("QUEUE_LISTEN_ADDR")
	if addr == "" {
		addr = ":50051"
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var serverOpts []grpc.ServerOption
	certFile := os.Getenv("GRPC_TLS_CERT_FILE")
	keyFile := os.Getenv("GRPC_TLS_KEY_FILE")
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
	} else {
		fmt.Println("Queue gRPC server TLS disabled — set GRPC_TLS_CERT_FILE + GRPC_TLS_KEY_FILE to enable")
	}

	s := grpc.NewServer(serverOpts...)
	broker := server.NewBroker()
	grpcServer := server.NewQueueGRPCServer(broker)

	grpcapi.RegisterQueueServiceServer(s, grpcServer)

	fmt.Printf("Queue gRPC server listening on %s\n", addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
