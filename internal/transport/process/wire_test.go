package process

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func rawFrame(s string) []byte {
	b := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(b[:4], uint32(len(s)))
	copy(b[4:], s)
	return b
}

func TestFrameRejectsMalformedBoundaries(t *testing.T) {
	cases := map[string][]byte{
		"empty length":     {0, 0, 0, 0},
		"oversize header":  {0x7f, 0xff, 0xff, 0xff},
		"truncated header": {0, 1},
		"truncated body":   {0, 0, 0, 9, '{'},
		"invalid UTF8":     rawFrame("{\"version\":\"1\",\"kind\":\"\xff\",\"binding\":{}}"),
		"wrong version":    rawFrame(`{"version":"9","kind":"job","binding":{}}`),
		"unknown field":    rawFrame(`{"version":"1","kind":"job","binding":{},"secret":"test-only"}`),
		"duplicate key":    rawFrame(`{"version":"1","version":"1","kind":"job","binding":{}}`),
		"case duplicate":   rawFrame(`{"version":"1","Version":"1","kind":"job","binding":{}}`),
		"nested duplicate": rawFrame(`{"version":"1","kind":"job","binding":{"task":"a","Task":"b"}}`),
		"trailing JSON":    rawFrame(`{"version":"1","kind":"job","binding":{}} {}`),
		"deep nesting":     rawFrame(strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readFrame(bytes.NewReader(b)); !errors.Is(err, ErrWire) {
				t.Fatalf("accepted invalid frame: %v", err)
			}
		})
	}
	if _, err := readFrame(bytes.NewReader(nil)); err != io.EOF {
		t.Fatal(err)
	}
}

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(b []byte) (int, error) {
	if len(b) > 3 {
		b = b[:3]
	}
	return w.Buffer.Write(b)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestFrameRoundTripAndPartialWrites(t *testing.T) {
	f := frame{Version: WireVersion, Kind: "continue", Binding: binding{Task: "t", Operation: "o", Attempt: "a", Generation: 1}}
	var w shortWriter
	if err := writeFrame(&w, f); err != nil {
		t.Fatal(err)
	}
	got, err := readFrame(&w)
	if err != nil || got != f {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if err := writeFrame(zeroWriter{}, f); !errors.Is(err, ErrWire) {
		t.Fatal(err)
	}
	f.Reason = strings.Repeat("x", MaxFrame)
	if err := writeFrame(io.Discard, f); !errors.Is(err, ErrWire) {
		t.Fatal(err)
	}
}

func TestFrameHeaderMatchesEncodedSize(t *testing.T) {
	f := frame{Version: WireVersion, Kind: "result", Reason: "синтетический пример"}
	var b bytes.Buffer
	if err := writeFrame(&b, f); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(f)
	if int(binary.BigEndian.Uint32(b.Bytes()[:4])) != len(encoded) {
		t.Fatal("length is not bytes")
	}
}
