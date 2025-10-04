package grpc_handler

import (
	"context"
	"franz/compiled_protos"
	"franz/franz-server/storage_handler"
)

type QueueHandler struct{}

func (q *QueueHandler) Enqueue(ctx *context.Context, entries *compiled_protos.DataEntryArray) (*compiled_protos.EnqueueResponse, error) {
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

func (q *QueueHandler) Dequeue(ctx *context.Context, dequeueRequest *compiled_protos.DequeueRequest) (*compiled_protos.DataEntryArray, error) {

}
