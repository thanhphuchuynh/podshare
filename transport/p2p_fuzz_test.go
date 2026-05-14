package transport

import (
	"bytes"
	"testing"
)

// FuzzReadFrame guards the wire parser against malicious peers. It should
// never panic, never block forever, and never return a successful parse
// with payload that exceeds the frame envelope.
//
// Run with: go test -fuzz=FuzzReadFrame -run=^$ ./transport
func FuzzReadFrame(f *testing.F) {
	// Seed corpus from a valid frame plus a few pathological ones.
	good := mustFrame(p2pMsgEvent, "topic", []byte("hello"))
	f.Add(good)
	f.Add(mustFrame(p2pMsgSnapReq, "", nil))
	f.Add(mustFrame(p2pMsgSnapResp, "very/long/topic", bytes.Repeat([]byte("x"), 1024)))
	f.Add([]byte{}) // empty
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00}) // total len = MaxUint32
	f.Add([]byte{0x00, 0x00, 0x00, 0x02, 0x01, 0xff})       // claims totalLen=2 < 3

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		msgType, topic, payload, err := readFrame(r)
		if err != nil {
			return
		}
		// On success, payload length must fit inside what was declared.
		if len(topic)+len(payload)+3 > len(data)-4 {
			t.Fatalf("decoded payload exceeds frame: type=%d topic=%q payload=%d frame=%d",
				msgType, topic, len(payload), len(data))
		}
	})
}

func mustFrame(msgType byte, topic string, payload []byte) []byte {
	var buf bytes.Buffer
	if err := writeFrame(&buf, msgType, topic, payload); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
