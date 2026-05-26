package podshare_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanhphuchuynh/podshare"
	"github.com/thanhphuchuynh/podshare/transport"
)

// v1 of the Flag schema — the receiver's struct.
type flagV1 struct {
	Enabled bool `json:"enabled"`
	Rollout int  `json:"rollout"`
}

// v2 of the Flag schema — what a future-version peer sends.
// Note `ExpireAt` is not present in v1, so a strict decoder must reject it.
type flagV2 struct {
	Enabled  bool   `json:"enabled"`
	Rollout  int    `json:"rollout"`
	ExpireAt string `json:"expire_at,omitempty"`
}

func TestStrictJSONCodec_AcceptsMatchingSchema(t *testing.T) {
	c := podshare.StrictJSONCodec{}
	raw, err := c.Marshal(flagV1{Enabled: true, Rollout: 50})
	if err != nil {
		t.Fatal(err)
	}
	var got flagV1
	if err := c.Unmarshal(raw, &got); err != nil {
		t.Fatalf("strict decode on matching schema: %v", err)
	}
	if !got.Enabled || got.Rollout != 50 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestStrictJSONCodec_AcceptsMissingFields(t *testing.T) {
	// v2 reading a v1 payload — v2's extra fields just stay as zero.
	// This MUST work or rolling deploys are impossible.
	c := podshare.StrictJSONCodec{}
	raw, err := c.Marshal(flagV1{Enabled: true, Rollout: 50})
	if err != nil {
		t.Fatal(err)
	}
	var got flagV2
	if err := c.Unmarshal(raw, &got); err != nil {
		t.Fatalf("strict decode rejected a v1-payload-into-v2-struct read: %v", err)
	}
	if got.ExpireAt != "" {
		t.Fatalf("ExpireAt should be zero, got %q", got.ExpireAt)
	}
}

func TestStrictJSONCodec_RejectsUnknownFields(t *testing.T) {
	// v1 reading a v2 payload — this is the case we want to catch.
	c := podshare.StrictJSONCodec{}
	raw, err := c.Marshal(flagV2{Enabled: true, Rollout: 50, ExpireAt: "2026-01-01"})
	if err != nil {
		t.Fatal(err)
	}
	var got flagV1
	err = c.Unmarshal(raw, &got)
	if err == nil {
		t.Fatal("strict decode silently dropped an unknown field")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error doesn't mention unknown field: %v", err)
	}
}

func TestStrictCodecSurfacesDriftViaOnError(t *testing.T) {
	// End-to-end: a peer publishes a v2 payload, the v1-shaped receiver
	// is using StrictJSONCodec. The decode failure should fire OnError
	// rather than silently apply a zero-Flag.
	tr := transport.NewMemoryTransport()
	defer tr.Close()
	ctx := context.Background()

	var errsMu sync.Mutex
	var errs []error
	receiver, err := podshare.New[flagV1](ctx, "drift", tr,
		podshare.WithNodeID[flagV1]("v1-pod"),
		podshare.WithCodec[flagV1](podshare.StrictJSONCodec{}),
		podshare.WithErrorHandler[flagV1](func(e error) {
			errsMu.Lock()
			defer errsMu.Unlock()
			errs = append(errs, e)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()

	// A "v2" peer crafts a payload by hand and publishes it. We can't use
	// a real Store[flagV2] on the same transport because the receiver
	// would unmarshal the wire envelope's V correctly — we want to
	// poison the inner value bytes.
	v2payload := []byte(`{"enabled":true,"rollout":50,"expire_at":"2026-01-01"}`)
	msg := []byte(`{"v":1,"kind":"set","key":"k","value":` +
		toBase64JSON(v2payload) +
		`,"origin":"v2-pod","timestamp":"2026-05-18T00:00:00Z","seq":1}`)
	if err := tr.Publish(ctx, "drift", msg); err != nil {
		t.Fatal(err)
	}

	// Give the receiver a beat to process.
	time.Sleep(60 * time.Millisecond)

	// 1. The strict decode should have surfaced via OnError.
	errsMu.Lock()
	defer errsMu.Unlock()
	if len(errs) == 0 {
		t.Fatal("expected an OnError call from strict decode")
	}
	if !errorsContains(errs, "unknown field") {
		t.Fatalf("expected an 'unknown field' error, got %v", errs)
	}

	// 2. The key MUST NOT be applied — strict decode rejected the value.
	if _, ok := receiver.Get("k"); ok {
		t.Fatal("strict decode failed to reject the v2 payload; value was applied")
	}
}

// toBase64JSON emits a JSON-quoted base64 string for embedding raw bytes
// in a JSON document. encoding/json marshals []byte as base64.
func toBase64JSON(b []byte) string {
	type wrap struct{ V []byte }
	out, _ := podshare.JSONCodec{}.Marshal(struct{ V []byte }{V: b})
	// out is `{"V":"base64..."}`; strip the wrapper.
	s := string(out)
	start := strings.Index(s, `"V":"`) + len(`"V":"`)
	end := strings.LastIndex(s, `"`)
	return `"` + s[start:end] + `"`
}

func errorsContains(errs []error, needle string) bool {
	for _, e := range errs {
		if e != nil && strings.Contains(e.Error(), needle) {
			return true
		}
	}
	return false
}

// guard against import-cycle weirdness in test refactors.
var _ = errors.New
