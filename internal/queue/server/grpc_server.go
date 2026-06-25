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
	msgs := make([]Message, len(req.Messages))
	for i, mp := range req.Messages {
		msgs[i] = Message{Key: mp.Key, Value: mp.Value}
	}
	return &grpc.ProduceResponse{Offsets: topic.Produce(msgs)}, nil
}

func (s *QueueGRPCServer) StreamConsume(req *grpc.ConsumeStreamRequest, stream grpc.QueueService_StreamConsumeServer) error {
	topic := s.broker.GetTopic(req.Topic)
	maxBatch := int(req.MaxBatchSize)
	if maxBatch <= 0 {
		maxBatch = 500
	}

	for {
		msgs, err := topic.ConsumeBatch(req.GroupId, maxBatch, stream.Context())
		if err != nil {
			return err // context cancelled (shutdown) or permanent error
		}

		batch := make([]*grpc.BatchedMessage, len(msgs))
		for i, m := range msgs {
			batch[i] = &grpc.BatchedMessage{Key: m.Msg.Key, Value: m.Msg.Value, Offset: m.Offset}
		}

		if err := stream.Send(&grpc.ConsumeStreamResponse{Messages: batch}); err != nil {
			return err
		}
	}
}

func (s *QueueGRPCServer) Commit(ctx context.Context, req *grpc.CommitRequest) (*grpc.CommitResponse, error) {
	topic := s.broker.GetTopic(req.Topic)
	if err := topic.Commit(req.GroupId, req.Offset); err != nil {
		return nil, fmt.Errorf("commit error: %w", err)
	}
	return &grpc.CommitResponse{Success: true}, nil
}
