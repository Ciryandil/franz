package queue

import "github.com/Ciryandil/franz/custom_errors"

type Node struct {
	Key         string
	Next        *Node
	FileOffset  int64
	DataSize    int64
	QueueOffset int64
	Timestamp   int64
}

type Queue struct {
	Head   *Node
	Tail   *Node
	Offset int64
}

type QueueHandler struct {
	DataQueue   *Queue
	BufferQueue *Queue
}

func NewQueue() *Queue {
	queue := Queue{
		Head: nil,
		Tail: nil,
	}
	return &queue
}

func (q *Queue) Enqueue(node *Node) {
	node.QueueOffset = q.Offset
	if q.Tail == nil {
		q.Tail = node
		q.Head = node
	} else {
		q.Tail.Next = node
		q.Tail = node
	}

	q.Offset += 1
}

func (q *Queue) Dequeue() (*Node, error) {
	if q.Head == nil {
		return nil, custom_errors.ErrQueueEmpty
	}
	currHead := q.Head
	q.Head = q.Head.Next
	return currHead, nil
}

func (qh *QueueHandler) NewQueueHandler() *QueueHandler {
	handler := QueueHandler{
		DataQueue:   NewQueue(),
		BufferQueue: NewQueue(),
	}
	return &handler
}

func (qh *QueueHandler) Enqueue(key string, fileOffset int64, timestamp int64) {
	node, err := qh.BufferQueue.Dequeue()
	if err != nil {
		node = &Node{}
	}
	node.Key = key
	node.FileOffset = fileOffset
	node.Timestamp = timestamp
	node.Next = nil
	qh.DataQueue.Enqueue(node)
}

func (qh *QueueHandler) Dequeue() (int64, error) {
	node, err := qh.DataQueue.Dequeue()
	if err != nil {
		return -1, err
	}
	fileOffset := node.FileOffset
	qh.BufferQueue.Enqueue(node)
	return fileOffset, nil
}
