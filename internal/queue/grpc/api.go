package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// Hand-written gRPC bindings — no protoc dependency.
// StreamConsume is server-streaming: broker pushes batches, eliminating the per-message RTT (~500k msg/s vs ~1k for unary).

type ProduceRequest struct {
	Topic    string
	Messages []*MessagePayload
}

type MessagePayload struct {
	Key   []byte
	Value []byte
}

type ProduceResponse struct {
	Offsets []int64
}

type ConsumeStreamRequest struct {
	Topic        string
	GroupId      string
	MaxBatchSize int32 // max messages per stream frame; broker clips to its own limit
}

type BatchedMessage struct {
	Key    []byte
	Value  []byte
	Offset int64
}

type ConsumeStreamResponse struct {
	Messages []*BatchedMessage
}

type CommitRequest struct {
	Topic   string
	GroupId string
	Offset  int64
}

type CommitResponse struct {
	Success bool
}

// --- server-side stream interface ---

type QueueService_StreamConsumeServer interface {
	Send(*ConsumeStreamResponse) error
	grpc.ServerStream
}

type queueServiceStreamConsumeServer struct{ grpc.ServerStream }

func (s *queueServiceStreamConsumeServer) Send(m *ConsumeStreamResponse) error {
	return s.ServerStream.SendMsg(m)
}

// --- client-side stream interface ---

// Recv returns the next batch; io.EOF = stream ended, codes.Unavailable = broker restarting.
type QueueService_StreamConsumeClient interface {
	Recv() (*ConsumeStreamResponse, error)
	grpc.ClientStream
}

type queueServiceStreamConsumeClient struct{ grpc.ClientStream }

func (c *queueServiceStreamConsumeClient) Recv() (*ConsumeStreamResponse, error) {
	m := new(ConsumeStreamResponse)
	if err := c.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// --- service interfaces ---

type QueueServiceClient interface {
	Produce(ctx context.Context, in *ProduceRequest, opts ...grpc.CallOption) (*ProduceResponse, error)
	StreamConsume(ctx context.Context, in *ConsumeStreamRequest, opts ...grpc.CallOption) (QueueService_StreamConsumeClient, error)
	Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error)
}

type QueueServiceServer interface {
	Produce(context.Context, *ProduceRequest) (*ProduceResponse, error)
	StreamConsume(*ConsumeStreamRequest, QueueService_StreamConsumeServer) error
	Commit(context.Context, *CommitRequest) (*CommitResponse, error)
}

// --- client implementation ---

func NewQueueServiceClient(cc grpc.ClientConnInterface) QueueServiceClient {
	return &queueServiceClient{cc}
}

type queueServiceClient struct{ cc grpc.ClientConnInterface }

func (c *queueServiceClient) Produce(ctx context.Context, in *ProduceRequest, opts ...grpc.CallOption) (*ProduceResponse, error) {
	out := new(ProduceResponse)
	if err := c.cc.Invoke(ctx, "/queue.QueueService/Produce", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

var streamConsumeDesc = &grpc.StreamDesc{StreamName: "StreamConsume", ServerStreams: true}

func (c *queueServiceClient) StreamConsume(ctx context.Context, in *ConsumeStreamRequest, opts ...grpc.CallOption) (QueueService_StreamConsumeClient, error) {
	stream, err := c.cc.NewStream(ctx, streamConsumeDesc, "/queue.QueueService/StreamConsume", opts...)
	if err != nil {
		return nil, err
	}
	cs := &queueServiceStreamConsumeClient{stream}
	if err := cs.SendMsg(in); err != nil {
		return nil, err
	}
	if err := cs.CloseSend(); err != nil {
		return nil, err
	}
	return cs, nil
}

func (c *queueServiceClient) Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error) {
	out := new(CommitResponse)
	if err := c.cc.Invoke(ctx, "/queue.QueueService/Commit", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

// --- server registration ---

func produceHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(ProduceRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(QueueServiceServer).Produce(ctx, req)
}

func streamConsumeHandler(srv any, stream grpc.ServerStream) error {
	req := new(ConsumeStreamRequest)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	return srv.(QueueServiceServer).StreamConsume(req, &queueServiceStreamConsumeServer{stream})
}

func commitHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(CommitRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(QueueServiceServer).Commit(ctx, req)
}

var QueueServiceDesc = grpc.ServiceDesc{
	ServiceName: "queue.QueueService",
	HandlerType: (*QueueServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Produce", Handler: produceHandler},
		{MethodName: "Commit", Handler: commitHandler},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamConsume",
			Handler:       streamConsumeHandler,
			ServerStreams: true,
		},
	},
	Metadata: map[string]string{},
}

func RegisterQueueServiceServer(s *grpc.Server, srv QueueServiceServer) {
	s.RegisterService(&QueueServiceDesc, srv)
}
