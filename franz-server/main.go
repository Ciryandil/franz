package main

import (
	"context"
	"fmt"
	"franz/compiled_protos"
	"franz/franz-server/constants"
	"franz/franz-server/grpc_handler"
	"franz/franz-server/storage_handler"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGHUP,
	)
	defer stop()

	constants.InitConstants()
	storage_handler.InitializeHandlers()

	port := os.Getenv("FRANZ_SERVER_PORT")
	if port == "" {
		port = "50051"
	}
	lis, _ := net.Listen("tcp", fmt.Sprintf(":%s", port))
	s := grpc.NewServer()
	compiled_protos.RegisterQueueServiceServer(s, &grpc_handler.QueueHandler{})

	health := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, health)
	health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	s.Serve(lis)

	go func() {
		for range ctx.Done() {
			fmt.Println("[FRANZ] Shutting down...")
			s.GracefulStop()
			storage_handler.ConsumerOffsetHandler.Close()
			return
		}
	}()

	<-ctx.Done()
}
