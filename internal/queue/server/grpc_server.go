package server

import (
	"context"
	"fmt"

	"gpu-telemetry-pipeline/internal/queue/grpc"
)

type QueueGRPCServer struct {
	broker *Broker
}

func NewQueueGRPCServer(broker *Broker) *QueueGRPCServer {
	return &QueueGRPCServer{broker: broker}
}

func (s *QueueGRPCServer) Produce(ctx context.Context, req *grpc.ProduceRequest) (*grpc.ProduceResponse, error) {
	topic := s.broker.GetTopic(req.Topic)

	msgs := make([]Message, 0, len(req.Messages))
	for _, mp := range req.Messages {
		msgs = append(msgs, Message{
			Key:   mp.Key,
			Value: mp.Value,
		})
	}

	offsets := topic.Produce(msgs)

	return &grpc.ProduceResponse{Offsets: offsets}, nil
}

func (s *QueueGRPCServer) Consume(ctx context.Context, req *grpc.ConsumeRequest) (*grpc.ConsumeResponse, error) {
	topic := s.broker.GetTopic(req.Topic)

	wait := func() bool { return ctx.Err() == nil }

	msg, offset, err := topic.Consume(req.GroupId, wait)
	if err != nil {
		return nil, fmt.Errorf("consume error: %w", err)
	}

	return &grpc.ConsumeResponse{
		Key:    msg.Key,
		Value:  msg.Value,
		Offset: offset,
	}, nil
}

func (s *QueueGRPCServer) Commit(ctx context.Context, req *grpc.CommitRequest) (*grpc.CommitResponse, error) {
	topic := s.broker.GetTopic(req.Topic)
	err := topic.Commit(req.GroupId, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("commit error: %w", err)
	}

	return &grpc.CommitResponse{Success: true}, nil
}
