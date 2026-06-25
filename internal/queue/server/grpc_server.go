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

// StreamConsume delivers messages to the client using a two-goroutine pipeline:
//
//   dispatcher goroutine — reads batches from the ring buffer and pushes them
//   onto batchCh. It runs independently of the network write so it can pre-fetch
//   the next batch while the main goroutine is still serialising and sending the
//   previous one.
//
//   main goroutine — ranges over batchCh, encodes each batch, and calls
//   stream.Send. When stream.Context() is cancelled the dispatcher unblocks
//   (ConsumeBatch selects on ctx.Done()) and closes batchCh, which exits the
//   range loop cleanly.
func (s *QueueGRPCServer) StreamConsume(req *grpc.ConsumeStreamRequest, stream grpc.QueueService_StreamConsumeServer) error {
	topic := s.broker.GetTopic(req.Topic)
	maxBatch := int(req.MaxBatchSize)
	if maxBatch <= 0 {
		maxBatch = 500
	}

	// batchCh depth of 4 lets the dispatcher stay up to 4 batches ahead of the
	// sender — enough to hide network latency without unbounded memory growth.
	batchCh := make(chan []BatchedMsg, 4)
	go func() {
		defer close(batchCh)
		for {
			msgs, err := topic.ConsumeBatch(req.GroupId, maxBatch, stream.Context())
			if err != nil {
				return // context cancelled or stream gone
			}
			select {
			case batchCh <- msgs:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	for batch := range batchCh {
		out := make([]*grpc.BatchedMessage, len(batch))
		for i, m := range batch {
			out[i] = &grpc.BatchedMessage{Key: m.Msg.Key, Value: m.Msg.Value, Offset: m.Offset}
		}
		if err := stream.Send(&grpc.ConsumeStreamResponse{Messages: out}); err != nil {
			return err
		}
	}
	return stream.Context().Err()
}

func (s *QueueGRPCServer) Commit(ctx context.Context, req *grpc.CommitRequest) (*grpc.CommitResponse, error) {
	topic := s.broker.GetTopic(req.Topic)
	if err := topic.Commit(req.GroupId, req.Offset); err != nil {
		return nil, fmt.Errorf("commit error: %w", err)
	}
	return &grpc.CommitResponse{Success: true}, nil
}
