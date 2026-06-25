package grpc

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// jsonCodec replaces gRPC's default protobuf codec so that the hand-written
// request/response types (which don't implement proto.Message) can be
// marshaled with standard encoding/json. Named "proto" to override the
// default; both client and server import this package so init() runs on both
// sides, keeping the wire format consistent.
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)    { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                      { return "proto" }

func init() {
	encoding.RegisterCodec(jsonCodec{})
}
