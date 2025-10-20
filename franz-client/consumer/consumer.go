package consumer

import (
	"context"
	"fmt"
	"franz/compiled_protos"
	"franz/franz-client/utils"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

type ConsumerConfig struct {
	PartitionAddr  string
	CommitInterval uint16
	BatchSize      uint8
	Id             string
}

type Consumer struct {
	Config ConsumerConfig
	buffer chan *compiled_protos.DataEntry
	client *compiled_protos.QueueServiceClient
	conn   *grpc.ClientConn
	ctx    context.Context
	cancel context.CancelFunc
	offset int64
	lock   chan struct{}
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if config.Id == "" {
		return nil, fmt.Errorf("consumer id cannot be empty")
	}
	if config.PartitionAddr == "" {
		return nil, fmt.Errorf("partition address cannot be empty")
	}
	if config.CommitInterval == 0 {
		config.CommitInterval = 30
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1
	}
	conn := utils.ProbePartitionServer(config.PartitionAddr)
	if conn == nil {
		return nil, fmt.Errorf("failed to create connection to partition server")
	}
	client := compiled_protos.NewQueueServiceClient(conn)
	offsetCtx, offsetCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer offsetCancel()
	offsetResp, err := client.GetOffset(offsetCtx, &compiled_protos.GetOffsetRequest{
		Consumer: config.Id,
	})
	if err != nil {
		return nil, err
	}
	offset := offsetResp.Offset
	ctx, cancel := context.WithCancel(context.Background())
	consumer := Consumer{
		Config: config,
		buffer: make(chan *compiled_protos.DataEntry, 2*config.BatchSize),
		client: &client,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		offset: offset,
		lock:   make(chan struct{}, 1),
	}
	consumer.StartCommitLoop()
	return &consumer, nil
}

func (consumer *Consumer) StartCommitLoop() {
	go func() {
		ticker := time.NewTicker(time.Duration(consumer.Config.CommitInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-consumer.ctx.Done():
				return
			case <-ticker.C:
				consumer.CommitOffset()
			}
		}
	}()
}

func (consumer *Consumer) Consume() (*compiled_protos.DataEntry, bool) {
	var data *compiled_protos.DataEntry
	select {
	case data, _ = <-consumer.buffer:
		//pass
	default:
		consumer.pull()
		select {
		case data, _ = <-consumer.buffer:
			//pass
		default:
			return nil, false
		}
	}
	if data != nil {
		atomic.AddInt64(&consumer.offset, 1)
		return data, true
	}
	return nil, false
}

func (consumer *Consumer) pull() {
	select {
	case consumer.lock <- struct{}{}:
		defer func() { <-consumer.lock }()
	default:
		return
	}
	client := *consumer.client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.Dequeue(ctx, &compiled_protos.DequeueRequest{
		Offset:     consumer.offset + 1,
		NumEntries: int64(consumer.Config.BatchSize),
	})
	if err != nil {
		fmt.Printf("[FRANZ] Error consuming from consumer: %v\n", err)
		return
	}
	for _, entry := range resp.Entries {
		consumer.buffer <- entry
	}
}

func (consumer *Consumer) CommitOffset() {
	client := *consumer.client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.SetOffset(ctx, &compiled_protos.SetOffsetRequest{
		Consumer: consumer.Config.Id,
		Offset:   consumer.offset,
	})
	if err != nil {
		fmt.Printf("[FRANZ] Error setting consumer offset on partition: %v\n", err)
		return
	}
	if !resp.Success {
		fmt.Printf("[FRANZ] Consumer offset commit failed.\n")
	}
}

func (consumer *Consumer) Close() {
	consumer.cancel()
	close(consumer.buffer)
	consumer.CommitOffset()
}
