package transport

import (
	"bytes"
	"io"
	"testing"
)

// BenchmarkWriteFrame measures the cost of serializing a single P2P
// frame. Internal benchmark — exercises the unexported writeFrame.
func BenchmarkWriteFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	var buf bytes.Buffer
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf.Reset()
		if err := writeFrame(&buf, p2pMsgEvent, "topic", payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadFrame measures parsing cost.
func BenchmarkReadFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	var buf bytes.Buffer
	if err := writeFrame(&buf, p2pMsgEvent, "topic", payload); err != nil {
		b.Fatal(err)
	}
	frame := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := bytes.NewReader(frame)
		if _, _, _, err := readFrame(r); err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoundTripFrame measures encode + decode together to give a
// per-message cost figure for the framing layer.
func BenchmarkRoundTripFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 256)
	var buf bytes.Buffer
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf.Reset()
		if err := writeFrame(&buf, p2pMsgEvent, "topic", payload); err != nil {
			b.Fatal(err)
		}
		if _, _, _, err := readFrame(&buf); err != nil {
			b.Fatal(err)
		}
	}
}
