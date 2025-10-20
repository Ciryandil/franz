package producer

import (
	"context"
	"fmt"
	"franz/compiled_protos"
	"franz/franz-client/utils"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type ProducerConfig struct {
	PartitionAddrs []string
	FlushInterval  uint16
	BatchSize      uint8
}

type Producer struct {
	Config     ProducerConfig
	buffer     chan *compiled_protos.DataEntry
	ctx        context.Context
	cancel     context.CancelFunc
	bufferSize int32
	clients    []*compiled_protos.QueueServiceClient
	conns      []*grpc.ClientConn
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	addrs := config.PartitionAddrs
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no partition addresses provided")
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = 10
	}
	wg := sync.WaitGroup{}
	connChan := make(chan *grpc.ClientConn, len(addrs))
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			conn := utils.ProbePartitionServer(addr)
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
		buffer:  make(chan *compiled_protos.DataEntry, 2*config.BatchSize),
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
		ticker := time.NewTicker(time.Duration(producer.Config.FlushInterval) * time.Second)
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
	select {
	case <-producer.ctx.Done():
		return
	default:
		producer.buffer <- data
	}
	newSize := atomic.AddInt32(&producer.bufferSize, 1)
	if newSize == int32(producer.Config.BatchSize) {
		producer.Flush()
	}
}

func (producer *Producer) Flush() {
	currBufferSize := producer.bufferSize
	dataMap := make([][]*compiled_protos.DataEntry, len(producer.clients))
	for range currBufferSize {
		entry, _ := <-producer.buffer
		if entry == nil {
			break
		}
		index := producer.findPartition(entry.Key)
		dataMap[index] = append(dataMap[index], entry)
	}
	atomic.AddInt32(&producer.bufferSize, -currBufferSize)
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

func (producer *Producer) FlushAll() {
	count := int32(0)
	dataMap := make([][]*compiled_protos.DataEntry, len(producer.clients))
	for entry := range producer.buffer {
		count += 1
		index := producer.findPartition(entry.Key)
		dataMap[index] = append(dataMap[index], entry)
	}
	atomic.AddInt32(&producer.bufferSize, -count)
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
	hash := hashStringToInt32(key)
	return uint8(hash % int32(len(producer.Config.PartitionAddrs)))
}

func hashStringToInt32(s string) int32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int32(h.Sum32())
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
	close(producer.buffer)
	producer.FlushAll()
	for _, conn := range producer.conns {
		conn.Close()
	}
}
