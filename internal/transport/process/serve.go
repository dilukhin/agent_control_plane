package process

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
	"unicode/utf8"

	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

func provider(id string) (execution.Provider, error) {
	switch id {
	case "test.echo":
		return execution.EchoProvider{}, nil
	case "test.digest":
		return execution.DigestProvider{}, nil
	default:
		return nil, execution.ErrUnsupported
	}
}

func validJob(j execution.Job, local *execution.Local) error {
	r := j.Request
	now := time.Now().UTC()
	if !boundedIdentity(j) {
		return execution.ErrLimit
	}
	if r.Version != execution.Version || r.MessageID == "" || r.AttemptID == "" ||
		r.LeaseID == "" || r.CorrelationID == "" || r.Revision == 0 || j.Generation == 0 ||
		r.OperationID != j.Descriptor.ID || r.IssuedAt.IsZero() || r.IssuedAt.After(now) ||
		!r.Deadline.After(now) || r.Deadline.Sub(r.IssuedAt) > r.Limits.Duration ||
		len(r.Input) > r.Limits.InputBytes || !utf8.ValidString(r.Input) || r.DataClass != "synthetic" {
		return execution.ErrInvalid
	}
	if err := j.Descriptor.Validate(); err != nil {
		return execution.ErrInvalid
	}
	return local.Accept(j)
}

func boundedIdentity(j execution.Job) bool {
	if len(j.Descriptor.ConflictScope) > 32 {
		return false
	}
	ids := []string{string(j.Worker), string(j.Descriptor.ID), string(j.Descriptor.TaskID),
		j.Descriptor.Name, j.Descriptor.TargetRef, j.Descriptor.VerificationPolicyRef,
		j.Descriptor.AuthorizationPolicyRef, string(j.Request.MessageID),
		string(j.Request.OperationID), string(j.Request.AttemptID), string(j.Request.LeaseID),
		string(j.Request.RetryOf), string(j.Request.CorrelationID), j.Request.Provider,
		j.Request.Model, j.Request.DataClass}
	ids = append(ids, j.Descriptor.ConflictScope...)
	for _, id := range ids {
		if len(id) > MaxIdentifier || !utf8.ValidString(id) {
			return false
		}
	}
	return true
}

// Serve handles one job using only the two bounded reference providers. The
// caller owns these streams; closing them must unblock I/O (OS pipes do).
// Store, credentials and arbitrary commands are not available in this API.
func Serve(ctx context.Context, input io.ReadCloser, output io.WriteCloser, actor protocol.ActorID, providerID string) error {
	p, err := provider(providerID)
	if err != nil {
		return err
	}
	local, err := execution.NewLocal(actor, p)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, execution.MaxDuration)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { input.Close(); output.Close() })
	defer stop()
	f, err := readFrame(input)
	if err != nil || f.Kind != "job" || f.Job == nil || f.Claim != nil || f.Reason != "" || f.PID != 0 {
		return ErrWire
	}
	j := *f.Job
	if bind(j) != f.Binding {
		return ErrWire
	}
	if err := validJob(j, local); err != nil {
		return err
	}
	ctx, cancelJob := context.WithDeadline(ctx, j.Request.Deadline)
	defer cancelJob()
	stopJob := context.AfterFunc(ctx, func() { input.Close(); output.Close() })
	defer stopJob()
	heartbeats := 0
	heartbeat := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if heartbeats >= execution.MaxHeartbeats {
			return execution.ErrLimit
		}
		heartbeats++
		if err := writeFrame(output, frame{Version: WireVersion, Kind: "heartbeat", Binding: bind(j), PID: os.Getpid()}); err != nil {
			return err
		}
		ack, err := readFrame(input)
		if err != nil || ack.Kind != "continue" || ack.Binding != bind(j) || ack.Job != nil || ack.Claim != nil || ack.PID != 0 || ack.Reason != "" {
			return ErrWire
		}
		return ctx.Err()
	}
	claim, err := local.Run(ctx, j, heartbeat)
	if err != nil {
		reason := "provider_failed"
		switch {
		case errors.Is(err, execution.ErrLimit):
			reason = "budget_exceeded"
		case errors.Is(err, execution.ErrUnsupported):
			reason = "unsupported"
		case errors.Is(err, context.DeadlineExceeded):
			reason = "timeout"
		case errors.Is(err, context.Canceled):
			reason = "cancelled"
		}
		return writeFrame(output, frame{Version: WireVersion, Kind: "error", Binding: bind(j), Reason: reason, PID: os.Getpid()})
	}
	return writeFrame(output, frame{Version: WireVersion, Kind: "result", Binding: bind(j), Claim: &claim, PID: os.Getpid()})
}
