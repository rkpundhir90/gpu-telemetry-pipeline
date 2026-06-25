package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
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

	s := grpc.NewServer()
	broker := server.NewBroker()
	grpcServer := server.NewQueueGRPCServer(broker)

	grpcapi.RegisterQueueServiceServer(s, grpcServer)

	fmt.Printf("Queue gRPC server listening on %s\n", addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
