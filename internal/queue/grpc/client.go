package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gpu-telemetry-pipeline/internal/queue"
)

// grpcServiceConfig enables wait-for-ready on all methods so that RPCs queue
// up during broker reconnection instead of failing immediately.
const grpcServiceConfig = `{
  "methodConfig": [{
    "name": [{"service": "queue.QueueService"}],
    "waitForReady": true
  }]
}`

// tlsDialOption returns TLS credentials when GRPC_TLS_CA_FILE is set, insecure otherwise.
func tlsDialOption() (grpc.DialOption, error) {
	caFile := os.Getenv("GRPC_TLS_CA_FILE")
	if caFile == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("grpc: read CA cert %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("grpc: invalid CA cert in %q", caFile)
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	})), nil
}

// isTransient returns true for errors that are likely caused by a broker
// restart rather than a permanent failure.
func isTransient(err error) bool {
	if err == io.EOF {
		return true
	}
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

func newConn(addr string) (*grpc.ClientConn, error) {
	opt, err := tlsDialOption()
	if err != nil {
		return nil, err
	}
	return grpc.NewClient(addr, opt, grpc.WithDefaultServiceConfig(grpcServiceConfig))
}

// --- producer ---

type producer struct {
	conn   *grpc.ClientConn
	client QueueServiceClient
	topic  string
}

func NewProducer(addr, topic string) (queue.Producer, error) {
	conn, err := newConn(addr)
	if err != nil {
		return nil, fmt.Errorf("grpc producer dial error: %w", err)
	}
	return &producer{conn: conn, client: NewQueueServiceClient(conn), topic: topic}, nil
}

// Publish retries on transient broker failures (pod restart) up to 5 times.
func (p *producer) Publish(ctx context.Context, msgs ...queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	payloads := make([]*MessagePayload, len(msgs))
	for i, m := range msgs {
		payloads[i] = &MessagePayload{Key: m.Key, Value: m.Value}
	}

	backoff := 100 * time.Millisecond
	for attempt := 0; attempt <= 5; attempt++ {
		_, err := p.client.Produce(ctx, &ProduceRequest{Topic: p.topic, Messages: payloads})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isTransient(err) || attempt == 5 {
			return fmt.Errorf("grpc produce error: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 8*time.Second)
	}
	return nil
}

func (p *producer) Close() error { return p.conn.Close() }

// --- consumer ---

// consumer wraps a server-streaming gRPC stream with an internal batch buffer.
// Reconnects transparently on broker restart; callers see no interruption.
type consumer struct {
	conn    *grpc.ClientConn
	client  QueueServiceClient
	topic   string
	groupID string

	// streaming state — only accessed from Fetch (single goroutine)
	stream QueueService_StreamConsumeClient
	buf    []*BatchedMessage
	bufIdx int
}

func NewConsumer(addr, topic, groupID string) (queue.Consumer, error) {
	conn, err := newConn(addr)
	if err != nil {
		return nil, fmt.Errorf("grpc consumer dial error: %w", err)
	}
	return &consumer{conn: conn, client: NewQueueServiceClient(conn), topic: topic, groupID: groupID}, nil
}

func (c *consumer) Fetch(ctx context.Context) (queue.Message, error) {
	for {
		if c.bufIdx < len(c.buf) {
			m := c.buf[c.bufIdx]
			c.bufIdx++
			return queue.NewMessage(m.Key, m.Value, m.Offset), nil
		}

		batch, err := c.recvBatch(ctx)
		if err != nil {
			return queue.Message{}, err
		}
		c.buf = batch
		c.bufIdx = 0
	}
}

// recvBatch returns the next batch, reopening the stream after transient errors.
func (c *consumer) recvBatch(ctx context.Context) ([]*BatchedMessage, error) {
	backoff := 100 * time.Millisecond
	for {
		if c.stream == nil {
			stream, err := c.client.StreamConsume(ctx, &ConsumeStreamRequest{
				Topic:        c.topic,
				GroupId:      c.groupID,
				MaxBatchSize: 500,
			})
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if isTransient(err) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(backoff):
					}
					backoff = min(backoff*2, 10*time.Second)
					continue
				}
				return nil, fmt.Errorf("grpc stream open: %w", err)
			}
			c.stream = stream
			backoff = 100 * time.Millisecond
		}

		resp, err := c.stream.Recv()
		if err != nil {
			c.stream = nil
			c.buf = nil
			c.bufIdx = 0
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if isTransient(err) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, 10*time.Second)
				continue
			}
			return nil, fmt.Errorf("grpc stream recv: %w", err)
		}
		return resp.Messages, nil
	}
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

func (c *consumer) Close() error { return c.conn.Close() }
