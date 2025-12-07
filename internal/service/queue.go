package service

import (
	"context"
	"errors"
	"fmt"
	"imageProcessor/internal/domain"
)

type ChannelQueue struct {
	tasks chan domain.ProcessingTask
}

func NewChannelQueue(bufferSize int) *ChannelQueue {
	return &ChannelQueue{
		tasks: make(chan domain.ProcessingTask, bufferSize),
	}
}

func (q *ChannelQueue) Enqueue(ctx context.Context, msg domain.ProcessingTask) error {
	select {
	case q.tasks <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *ChannelQueue) ConsumeMessage(ctx context.Context) (domain.ProcessingTask, error) {
	select {
	case task, ok := <-q.tasks:
		fmt.Println("000000000000", task)
		if !ok {
			return domain.ProcessingTask{}, errors.New("queue closed")
		}
		return task, nil
	case <-ctx.Done():
		return domain.ProcessingTask{}, ctx.Err()
	}
}

func (q *ChannelQueue) Commit(ctx context.Context, msg domain.ProcessingTask) error {
	// В случае с каналом явного подтверждения не требуется
	return nil
}

func (q *ChannelQueue) Close() {
	close(q.tasks)
}
