package utils

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func ProbePartitionServer(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[FRANZ] Error connecting to %s: %v\n", addr, err)
		return nil
	}
	client := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		fmt.Printf("[FRANZ] Error connecting to %s: %v\n", addr, err)
		return nil
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		fmt.Printf("[FRANZ] Error connecting to %s: server not serving\n", addr)
		return nil
	}
	return conn
}
