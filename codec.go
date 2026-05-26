package podshare

import (
	"bytes"
	"encoding/json"
)

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

// JSONCodec is the default Codec — encoding/json. Marshals values to
// JSON, unmarshal is permissive (unknown fields in the wire payload are
// silently dropped).
//
// The permissive behavior is fine for additive schema changes — old pods
// read new payloads without crashing — but it hides field-drop during
// rolling deploys with mixed versions of T. See StrictJSONCodec for the
// loud-failure variant.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// StrictJSONCodec is like JSONCodec but rejects payloads containing
// fields that aren't present in T. Wire it up with WithCodec when you
// want schema drift to surface immediately as OnError calls instead of
// silent field loss during mixed-version deploys:
//
//	store, _ := podshare.New[Flag](ctx, "flags", tr,
//	    podshare.WithCodec[Flag](podshare.StrictJSONCodec{}),
//	    podshare.WithErrorHandler[Flag](func(e error) {
//	        slog.Warn("podshare decode", "err", e)
//	    }),
//	)
//
// On a schema mismatch, the decode fails; handleRaw reports it via
// OnError and the message is skipped. State will diverge across
// versions while the mismatch persists — that's the point. Loud failure
// beats a silent dropped field.
//
// Pair StrictJSONCodec with a two-phase deploy: ship a version that
// READS the new field (still in strict mode — it'll accept payloads
// without the field, because they have *no extra* field) before any
// pod starts WRITING the new field. The strict check fires on unknown
// fields, not missing ones.
type StrictJSONCodec struct{}

func (StrictJSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (StrictJSONCodec) Unmarshal(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
