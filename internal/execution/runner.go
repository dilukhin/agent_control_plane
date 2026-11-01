package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

type Disposition string

const (
	Rejected  Disposition = "rejected"
	Duplicate Disposition = "duplicate"
	Reported  Disposition = "reported"
	Unknown   Disposition = "unknown_outcome"
)

type Reason string

const (
	OK          Reason = "ok"
	Unsupported Reason = "unsupported"
	Denied      Reason = "denied"
	Unavailable Reason = "unavailable"
	Budget      Reason = "budget_exceeded"
	TimedOut    Reason = "timeout"
	Cancelled   Reason = "cancelled"
	Failed      Reason = "executor_error"
)

func reason(err error) Reason {
	switch {
	case err == nil:
		return OK
	case errors.Is(err, ErrUnsupported):
		return Unsupported
	case errors.Is(err, ErrDenied):
		return Denied
	case errors.Is(err, ErrUnavailable):
		return Unavailable
	case errors.Is(err, ErrLimit):
		return Budget
	case errors.Is(err, context.DeadlineExceeded):
		return TimedOut
	case errors.Is(err, context.Canceled):
		return Cancelled
	default:
		return Failed
	}
}

type Report struct {
	At     time.Time
	Reason Reason
	Digest string
	Units  int
}
type Outcome struct {
	Disposition Disposition
	Reason      Reason
	Applied     bool
	Report      Report
	Output      string // transient caller-owned data; never persisted by Runner
}
type Runner struct {
	store      *core.Store
	controller protocol.ActorID
	worker     Worker
	policy     Policy
}

func NewRunner(s *core.Store, controller protocol.ActorID, w Worker, policy Policy) (*Runner, error) {
	if s == nil || controller == "" || w == nil || policy == nil {
		return nil, ErrInvalid
	}
	return &Runner{s, controller, w, policy}, nil
}
func digest(s string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(s))) }
func event(j Job, id protocol.MessageID, name string, actor protocol.ActorID, at time.Time, payload map[string]any) protocol.Envelope {
	return protocol.Envelope{ProtocolVersion: protocol.ProtocolVersion, MessageID: id, MessageType: protocol.MessageEvent, MessageName: name, TaskID: j.Descriptor.TaskID, OperationID: j.Descriptor.ID, AttemptID: j.Request.AttemptID, CorrelationID: j.Request.CorrelationID, CausationID: j.Request.MessageID, Actor: protocol.Actor{ID: actor, Role: "worker"}, IssuedAt: at, Lease: &protocol.LeaseRef{ID: j.Request.LeaseID, Generation: j.Generation}, Payload: payload}
}
func record(j Job, id string, kind evidence.Kind, actor protocol.ActorID, at time.Time, summary string) evidence.Record {
	return evidence.Record{ID: protocol.EvidenceID(id), TaskID: j.Descriptor.TaskID, OperationID: j.Descriptor.ID, AttemptID: j.Request.AttemptID, Kind: kind, SourceActorID: actor, ObservedAt: at, SubjectRef: j.Descriptor.TargetRef, Summary: summary}
}

// Execute dispatches at most once per committed offer. A duplicate acknowledges
// recorded state only; it does not resume work. Unknown outcomes never retry.
func (r *Runner) Execute(ctx context.Context, req Request) (Outcome, error) {
	now := time.Now().UTC()
	if err := req.validate(now); err != nil {
		return Outcome{Disposition: Rejected, Reason: reason(err)}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{Disposition: Rejected, Reason: reason(err)}, err
	}
	ctx, cancel := context.WithDeadline(ctx, req.Deadline)
	defer cancel()
	op, err := r.store.GetOperation(ctx, req.OperationID)
	if err != nil {
		return Outcome{}, err
	}
	c := r.worker.Capabilities()
	j := Job{Request: req, Descriptor: op.Descriptor, Worker: c.Worker}
	check := supports(c, j)
	if check == nil {
		check = r.policy.Check(j, c, now)
	}
	if check == nil && op.LeaseGeneration > 0 && op.State == state.OperationReady {
		prior, e := r.store.GetAttempt(ctx, req.RetryOf)
		if e != nil {
			if !errors.Is(e, core.ErrNotFound) {
				return Outcome{}, e
			}
			check = ErrDenied
		} else if prior.OperationID != req.OperationID || prior.LeaseGeneration != op.LeaseGeneration {
			check = ErrDenied
		}
	}
	if check == nil {
		check = r.worker.Accept(cloneJob(j))
	}
	why := reason(check)
	env := event(j, req.MessageID, "operation.offer", r.controller, req.IssuedAt, map[string]any{
		"version": req.Version, "worker": string(c.Worker), "provider": req.Provider, "model": req.Model, "data_class": req.DataClass,
		"input_digest": digest(req.Input), "revision": req.Revision, "retry_of": string(req.RetryOf),
		"duration_ns": int64(req.Limits.Duration), "input_bytes": req.Limits.InputBytes, "output_bytes": req.Limits.OutputBytes, "units": req.Limits.Units,
	})
	env.MessageType = protocol.MessageCommand
	env.Actor.Role = "controller"
	env.DeadlineAt = &req.Deadline
	env.Lease = &protocol.LeaseRef{ID: req.LeaseID} // generation assigned by core
	duplicate, err := r.store.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
		if check != nil {
			rec := record(j, string(req.MessageID)+"/rejected", evidence.KindPolicyDecision, r.controller, now, string(why))
			rec.AttemptID = "" // no execution authority was issued
			return tx.RegisterEvidence(rec)
		}
		_, a, e := tx.StartAttempt(req.OperationID, req.Revision, req.AttemptID, req.LeaseID, c.Worker, req.RetryOf, now)
		if e != nil {
			return e
		}
		j.Generation = a.LeaseGeneration
		return tx.RegisterEvidence(record(j, string(req.AttemptID)+"/accepted", evidence.KindPolicyDecision, r.controller, now, "accepted"))
	})
	if err != nil {
		return Outcome{}, err
	} // never dispatch after an uncertain commit
	if duplicate {
		return Outcome{Disposition: Duplicate}, nil
	}
	if check != nil {
		return Outcome{Disposition: Rejected, Reason: why}, nil
	}

	var mu sync.Mutex
	beats := 0
	closed := false
	heartbeat := func(callCtx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return core.ErrStaleOwnership
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := callCtx.Err(); err != nil {
			return err
		}
		at := time.Now().UTC()
		if !at.Before(req.Deadline) {
			return context.DeadlineExceeded
		}
		if err := r.policy.Check(j, c, at); err != nil {
			return ErrDenied
		}
		if beats >= MaxHeartbeats {
			return ErrLimit
		}
		id := fmt.Sprintf("%s/heartbeat/%d", req.AttemptID, beats)
		env := event(j, protocol.MessageID(id), "operation.progress", j.Worker, at, nil)
		_, err := r.store.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
			if err := tx.CheckActiveAttemptContext(req.OperationID, req.AttemptID, req.LeaseID, j.Generation, j.Worker); err != nil {
				return err
			}
			return tx.RegisterEvidence(record(j, id, evidence.KindWorkerReport, j.Worker, at, "heartbeat"))
		})
		if err == nil {
			beats++
		}
		return err
	}
	// Gate immediately before dispatch, in addition to the worker's checkpoints.
	if err := heartbeat(ctx); err != nil {
		return r.uncertain(j, reason(err))
	}
	type result struct {
		claim Claim
		err   error
	}
	done := make(chan result, 1)
	go func() {
		v := result{}
		defer func() {
			if recover() != nil {
				v = result{err: ErrProvider}
			}
			done <- v
		}()
		v.claim, v.err = r.worker.Run(ctx, cloneJob(j), heartbeat)
	}()
	var v result
	select {
	case v = <-done:
	case <-ctx.Done():
		v.err = ctx.Err()
	}
	mu.Lock()
	closed = true
	mu.Unlock()
	// Cancellation wins even if completion became ready at the same instant.
	if ctx.Err() != nil {
		v.err = ctx.Err()
	}
	if v.err == nil && (len(v.claim.Output) > req.Limits.OutputBytes || v.claim.Units < 0 || v.claim.Units > req.Limits.Units) {
		v.err = ErrLimit
	}
	if v.err == nil && r.policy.Check(j, c, time.Now().UTC()) != nil {
		v.err = ErrDenied
	}
	if v.err != nil {
		return r.uncertain(j, reason(v.err))
	}
	report := Report{At: time.Now().UTC(), Reason: OK, Digest: digest(v.claim.Output), Units: v.claim.Units}
	applied, dup, err := r.report(ctx, j, report)
	if err != nil {
		out, recoveryErr := r.uncertain(j, reason(err))
		return out, errors.Join(err, recoveryErr)
	}
	d := Reported
	if dup {
		d = Duplicate
	}
	return Outcome{Disposition: d, Reason: OK, Applied: applied, Report: report, Output: v.claim.Output}, nil
}
func cloneJob(j Job) Job {
	j.Descriptor.ConflictScope = slices.Clone(j.Descriptor.ConflictScope)
	return j
}

// report is a trusted local adapter boundary, not a network endpoint. Replay
// must retain the original Report.At and digest. No provider-selected kind,
// verifier identity or verdict is accepted.
func (r *Runner) report(ctx context.Context, j Job, v Report) (applied, duplicate bool, err error) {
	if v.Reason != OK || v.At.IsZero() || v.At.After(time.Now().Add(time.Second)) || v.At.Before(j.Request.IssuedAt) || v.Units < 0 || v.Units > j.Request.Limits.Units || len(v.Digest) != 71 || v.Digest[:7] != "sha256:" {
		return false, false, ErrInvalid
	}
	if _, err := hex.DecodeString(v.Digest[7:]); err != nil {
		return false, false, ErrInvalid
	}
	id := string(j.Request.AttemptID) + "/result"
	env := event(j, protocol.MessageID(id), "operation.result_reported", j.Worker, v.At, map[string]any{"reason": string(v.Reason), "digest": v.Digest, "units": v.Units})
	duplicate, err = r.store.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
		rec := record(j, id, evidence.KindWorkerReport, j.Worker, v.At, "result_reported")
		rec.Digest = v.Digest
		var e error
		applied, e = tx.RecordWorkerReport(rec, j.Request.LeaseID, j.Generation, j.Request.Deadline)
		return e
	})
	return
}
func (r *Runner) uncertain(j Job, why Reason) (Outcome, error) {
	out := Outcome{Disposition: Unknown, Reason: why}
	// Caller cancellation must not erase the recovery observation. This detached
	// storage operation is finite and never repeats execution or a failed write.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	op, err := r.store.GetOperation(ctx, j.Descriptor.ID)
	if err != nil {
		return out, err
	}
	if op.ActiveAttemptID != j.Request.AttemptID {
		return out, core.ErrStaleOwnership
	}
	now := time.Now().UTC()
	rec := record(j, string(j.Request.AttemptID)+"/lost", evidence.KindProcessObservation, r.controller, now, string(why))
	loss := core.OwnershipLost
	if why == TimedOut {
		loss = core.LeaseExpired
	}
	_, err = r.store.InvalidateAttempt(ctx, j.Descriptor.ID, op.Revision, j.Request.AttemptID, loss, rec, now)
	return out, err
}
