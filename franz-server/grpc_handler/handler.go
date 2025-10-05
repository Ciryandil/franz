package grpc_handler

import (
	"context"
	"franz/compiled_protos"
	"franz/franz-server/storage_handler"
	"sync"
)

type QueueHandler struct {
	*compiled_protos.UnimplementedQueueServiceServer
	EnqueueLock sync.Mutex
	DequeueLock sync.Mutex
}

func (q *QueueHandler) Enqueue(ctx context.Context, entries *compiled_protos.DataEntryArray) (*compiled_protos.EnqueueResponse, error) {
	q.EnqueueLock.Lock()
	defer q.EnqueueLock.Unlock()
	offsets, err := storage_handler.WriteEntriesToFile(entries.Entries)
	if err != nil {
		return nil, err
	}
	err = storage_handler.WriteOffsetsToFile(offsets)
	if err != nil {
		return nil, err
	}
	return &compiled_protos.EnqueueResponse{Success: true}, nil
}

func (q *QueueHandler) Dequeue(ctx context.Context, dequeueRequest *compiled_protos.DequeueRequest) (*compiled_protos.DataEntryArray, error) {
	q.DequeueLock.Lock()
	defer q.DequeueLock.Unlock()
	offsets, err := storage_handler.ReadOffsetsFromFile(uint64(dequeueRequest.NumEntries)+1, dequeueRequest.Offset*8)
	if err != nil {
		return nil, err
	}
	totalBytesToRead := offsets[len(offsets)-1] - offsets[0]
	resp, err := storage_handler.ReadEntriesFromFile(int64(offsets[0]), int64(totalBytesToRead))
	if err != nil {
		return nil, err
	}
	return resp, nil
}
