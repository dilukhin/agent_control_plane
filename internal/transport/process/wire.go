// Package process implements the bounded, local R5 process transport.
// It is not a sandbox, a remote authentication mechanism, or a scheduler.
package process

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

const WireVersion = "1"
const MaxFrame = 1024 * 1024
const MaxStderr = 4096
const MaxIdentifier = 256

var ErrWire = errors.New("invalid process transport frame")
var ErrCleanup = errors.New("process cleanup not confirmed within budget")

// Binding is checked on every response. Child-selected identity is never used
// to choose an operation, actor, lease or policy in the controller.
type binding struct {
	Task        protocol.TaskID        `json:"task"`
	Operation   protocol.OperationID   `json:"operation"`
	Attempt     protocol.AttemptID     `json:"attempt"`
	Lease       protocol.LeaseID       `json:"lease"`
	Generation  uint64                 `json:"generation"`
	Correlation protocol.CorrelationID `json:"correlation"`
	Offer       protocol.MessageID     `json:"offer"`
	Worker      protocol.ActorID       `json:"worker"`
	Provider    string                 `json:"provider"`
	Model       string                 `json:"model"`
}

func bind(j execution.Job) binding {
	return binding{j.Descriptor.TaskID, j.Request.OperationID, j.Request.AttemptID,
		j.Request.LeaseID, j.Generation, j.Request.CorrelationID, j.Request.MessageID,
		j.Worker, j.Request.Provider, j.Request.Model}
}

type frame struct {
	Version string           `json:"version"`
	Kind    string           `json:"kind"`
	Binding binding          `json:"binding"`
	Job     *execution.Job   `json:"job,omitempty"`
	Claim   *execution.Claim `json:"claim,omitempty"`
	Reason  string           `json:"reason,omitempty"`
	PID     int              `json:"pid,omitempty"`
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func writeFrame(w io.Writer, f frame) error {
	b, err := json.Marshal(f)
	if err != nil || len(b) == 0 || len(b) > MaxFrame {
		return ErrWire
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	if err := writeAll(w, header[:]); err != nil {
		return ErrWire
	}
	if err := writeAll(w, b); err != nil {
		return ErrWire
	}
	return nil
}

func readFrame(r io.Reader) (frame, error) {
	var f frame
	var header [4]byte
	n, err := io.ReadFull(r, header[:])
	if n == 0 && err == io.EOF {
		return f, io.EOF
	}
	if err != nil {
		return f, ErrWire
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxFrame {
		return f, ErrWire
	}
	b := make([]byte, int(size))
	if _, err := io.ReadFull(r, b); err != nil {
		return f, ErrWire
	}
	if !utf8.Valid(b) {
		return f, ErrWire
	}
	// encoding/json otherwise accepts duplicate and case-folded field names.
	keys := json.NewDecoder(bytes.NewReader(b))
	if err := uniqueJSON(keys, 0); err != nil {
		return f, ErrWire
	}
	if _, err := keys.Token(); err != io.EOF {
		return f, ErrWire
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&f); err != nil {
		return frame{}, ErrWire
	}
	if f.Version != WireVersion {
		return frame{}, ErrWire
	}
	return f, nil
}

func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 32 {
		return ErrWire
	}
	t, err := d.Token()
	if err != nil {
		return ErrWire
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			t, err := d.Token()
			k, ok := t.(string)
			if err != nil || !ok {
				return ErrWire
			}
			k = strings.ToLower(k)
			if seen[k] {
				return ErrWire
			}
			seen[k] = true
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrWire
		}
	case '[':
		for d.More() {
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrWire
		}
	default:
		return ErrWire
	}
	return nil
}
