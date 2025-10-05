package main

import (
	"flag"
	"fmt"
	"franz/compiled_protos"
	"franz/franz-server/grpc_handler"
	"net"

	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "port to listen on")
	lis, _ := net.Listen("tcp", fmt.Sprintf(":%d", port))
	s := grpc.NewServer()
	compiled_protos.RegisterQueueServiceServer(s, &grpc_handler.QueueHandler{})
	s.Serve(lis)
}
