package main

import (
	"context"
	"fmt"
	"franz/compiled_protos"
	"franz/franz-server/constants"
	"franz/franz-server/grpc_handler"
	"franz/franz-server/storage_handler"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file: ", err)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGHUP,
		syscall.SIGINT,
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
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to server: %v", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("[FRANZ] Shutting down...")
	s.GracefulStop()
	fmt.Println("[FRANZ] Shut down server")
	storage_handler.ConsumerOffsetHandler.Close()
	fmt.Println("[FRANZ] Wrote out consumer offsets.")
}
