package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gpu-telemetry-pipeline/internal/queue"
)

type producer struct {
	conn   *grpc.ClientConn
	client QueueServiceClient
	topic  string
}

func NewProducer(addr, topic string) (queue.Producer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc producer dial error: %w", err)
	}

	return &producer{
		conn:   conn,
		client: NewQueueServiceClient(conn),
		topic:  topic,
	}, nil
}

func (p *producer) Publish(ctx context.Context, msgs ...queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	payloads := make([]*MessagePayload, 0, len(msgs))
	for _, m := range msgs {
		payloads = append(payloads, &MessagePayload{
			Key:   m.Key,
			Value: m.Value,
		})
	}

	_, err := p.client.Produce(ctx, &ProduceRequest{
		Topic:    p.topic,
		Messages: payloads,
	})
	if err != nil {
		return fmt.Errorf("grpc produce error: %w", err)
	}

	return nil
}

func (p *producer) Close() error {
	return p.conn.Close()
}

type consumer struct {
	conn    *grpc.ClientConn
	client  QueueServiceClient
	topic   string
	groupID string
}

func NewConsumer(addr, topic, groupID string) (queue.Consumer, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc consumer dial error: %w", err)
	}

	return &consumer{
		conn:    conn,
		client:  NewQueueServiceClient(conn),
		topic:   topic,
		groupID: groupID,
	}, nil
}

func (c *consumer) Fetch(ctx context.Context) (queue.Message, error) {
	resp, err := c.client.Consume(ctx, &ConsumeRequest{
		Topic:   c.topic,
		GroupId: c.groupID,
	})
	if err != nil {
		return queue.Message{}, fmt.Errorf("grpc consume error: %w", err)
	}

	return queue.NewMessage(resp.Key, resp.Value, resp.Offset), nil
}

func (c *consumer) Commit(ctx context.Context, msgs ...queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	lastMsg := msgs[len(msgs)-1]
	offset, ok := lastMsg.Raw().(int64)
	if !ok {
		return fmt.Errorf("grpc: message is not committable (raw type %T)", lastMsg.Raw())
	}

	_, err := c.client.Commit(ctx, &CommitRequest{
		Topic:   c.topic,
		GroupId: c.groupID,
		Offset:  offset,
	})
	if err != nil {
		return fmt.Errorf("grpc commit error: %w", err)
	}

	return nil
}

func (c *consumer) Close() error {
	return c.conn.Close()
}
