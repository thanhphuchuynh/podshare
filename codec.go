package podshare

import "encoding/json"

// Codec encodes and decodes the per-key value type T to and from bytes
// before it crosses a Transport boundary or hits a snapshot. The default
// is JSONCodec; swap it via WithCodec for faster wire formats such as
// msgpack or protobuf.
//
// Codec methods must be safe for concurrent use.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
}

// JSONCodec is the default Codec — encoding/json.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
