package producer

import (
	"context"
	"fmt"
	"franz/compiled_protos"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type ProducerConfig struct {
	PartitionAddrs []string
	FlushFrequency uint16
	BatchSize      uint8
}

type Producer struct {
	Config     ProducerConfig
	buffer     chan *compiled_protos.DataEntry
	ctx        context.Context
	cancel     context.CancelFunc
	bufferSize uint32
	clients    []*compiled_protos.QueueServiceClient
	conns      []*grpc.ClientConn
}

func probePartitionServer(addr string) *grpc.ClientConn {
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

func NewProducer(config ProducerConfig) (*Producer, error) {
	addrs := config.PartitionAddrs
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no partition addresses provided")
	}
	wg := sync.WaitGroup{}
	connChan := make(chan *grpc.ClientConn, len(addrs))
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			conn := probePartitionServer(addr)
			if conn != nil {
				connChan <- conn
			}
			wg.Done()
		}(addr)
	}
	wg.Wait()
	close(connChan)
	reachableClients := make([]*compiled_protos.QueueServiceClient, 0)
	conns := make([]*grpc.ClientConn, 0)
	for conn := range connChan {
		conns = append(conns, conn)
		client := compiled_protos.NewQueueServiceClient(conn)
		reachableClients = append(reachableClients, &client)
	}
	if len(reachableClients) == 0 {
		return nil, fmt.Errorf("no partition addresses reachable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	producer := Producer{
		Config:  config,
		buffer:  make(chan *compiled_protos.DataEntry, config.BatchSize),
		ctx:     ctx,
		cancel:  cancel,
		clients: reachableClients,
		conns:   conns,
	}
	producerPtr := &producer
	producerPtr.StartFlushLoop()
	return producerPtr, nil
}

func (producer *Producer) StartFlushLoop() {
	go func() {
		ticker := time.NewTicker(time.Duration(producer.Config.FlushFrequency) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-producer.ctx.Done():
				return
			case <-ticker.C:
				producer.Flush()
			}
		}
	}()
}

func (producer *Producer) Produce(data *compiled_protos.DataEntry) {
	producer.buffer <- data
	newSize := atomic.AddUint32(&producer.bufferSize, 1)
	if newSize == uint32(producer.Config.BatchSize) {
		producer.Flush()
	}
}

func (producer *Producer) Flush() {
	currBufferSize := producer.bufferSize
	dataMap := make([][]*compiled_protos.DataEntry, len(producer.clients))
	for range currBufferSize {
		entry := <-producer.buffer
		index := producer.findPartition(entry.Key)
		dataMap[index] = append(dataMap[index], entry)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dataMap))
	for itr, dataEntries := range dataMap {
		dataEntryArray := &compiled_protos.DataEntryArray{
			Entries: dataEntries,
		}
		go producer.push(producer.clients[itr], dataEntryArray, &wg)
	}
	wg.Wait()
}

func (producer *Producer) findPartition(key string) uint8 {
	if key == "" {
		key = uuid.New().String()
	}
	hash := hashStringToUint32(key)
	return uint8(hash % uint32(len(producer.Config.PartitionAddrs)))
}

func hashStringToUint32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (producer *Producer) push(clientPtr *compiled_protos.QueueServiceClient, entries *compiled_protos.DataEntryArray, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := *clientPtr
	res, err := client.Enqueue(ctx, entries)
	if err != nil {
		fmt.Printf("[FRANZ] Enqueue failed: %v\n", err)
		return
	}
	if !res.Success {
		fmt.Printf("[FRANZ] Enqueue failed. Response indicates failure.")
	}
}

func (producer *Producer) Close() {
	producer.cancel()
	for _, conn := range producer.conns {
		conn.Close()
	}
}
