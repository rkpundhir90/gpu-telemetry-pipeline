package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// These structs mirror the protobuf definitions in proto/queue.proto
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

type ConsumeRequest struct {
	Topic   string
	GroupId string
}

type ConsumeResponse struct {
	Key    []byte
	Value  []byte
	Offset int64
}

type CommitRequest struct {
	Topic   string
	GroupId string
	Offset  int64
}

type CommitResponse struct {
	Success bool
}

// QueueServiceClient is the client API for QueueService
type QueueServiceClient interface {
	Produce(ctx context.Context, in *ProduceRequest, opts ...grpc.CallOption) (*ProduceResponse, error)
	Consume(ctx context.Context, in *ConsumeRequest, opts ...grpc.CallOption) (*ConsumeResponse, error)
	Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error)
}

// QueueServiceServer is the server API for QueueService
type QueueServiceServer interface {
	Produce(context.Context, *ProduceRequest) (*ProduceResponse, error)
	Consume(context.Context, *ConsumeRequest) (*ConsumeResponse, error)
	Commit(context.Context, *CommitRequest) (*CommitResponse, error)
}

func NewQueueServiceClient(cc grpc.ClientConnInterface) QueueServiceClient {
	return &queueServiceClient{cc}
}

type queueServiceClient struct {
	cc grpc.ClientConnInterface
}

func (c *queueServiceClient) Produce(ctx context.Context, in *ProduceRequest, opts ...grpc.CallOption) (*ProduceResponse, error) {
	out := new(ProduceResponse)
	err := c.cc.Invoke(ctx, "/queue.QueueService/Produce", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) Consume(ctx context.Context, in *ConsumeRequest, opts ...grpc.CallOption) (*ConsumeResponse, error) {
	out := new(ConsumeResponse)
	err := c.cc.Invoke(ctx, "/queue.QueueService/Consume", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitResponse, error) {
	out := new(CommitResponse)
	err := c.cc.Invoke(ctx, "/queue.QueueService/Commit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func produceHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(ProduceRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(QueueServiceServer).Produce(ctx, req)
}

func consumeHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(ConsumeRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(QueueServiceServer).Consume(ctx, req)
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
		{
			MethodName: "Produce",
			Handler:    produceHandler,
		},
		{
			MethodName: "Consume",
			Handler:    consumeHandler,
		},
		{
			MethodName: "Commit",
			Handler:    commitHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: map[string]string{},
}

func RegisterQueueServiceServer(s *grpc.Server, srv QueueServiceServer) {
	s.RegisterService(&QueueServiceDesc, srv)
}
