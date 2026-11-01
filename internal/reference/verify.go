// Package reference supplies only the fixed synthetic R5 verification policy.
// General domain verification remains the responsibility of trusted callers.
package reference

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

const Input = "R5 synthetic reference"
const Target = "fixture/r5-v1"
const Policy = "reference-check/v1"

// Verify compares actual received bytes with the fixed fixture, independently
// of the child's digest or exit code. A hash alone cannot reconstruct a lost
// result. Evidence and the verdict commit atomically through the existing R3
// message boundary; replay does not create a second verification.
func Verify(ctx context.Context, s *core.Store, req execution.Request, out execution.Outcome, verifier protocol.ActorID) (state.Operation, error) {
	if s == nil || verifier == "" || req.Input != Input || req.DataClass != "synthetic" || out.Disposition != execution.Reported || !out.Applied || out.Reason != execution.OK || out.Report.At.IsZero() {
		return state.Operation{}, execution.ErrInvalid
	}
	expected := ""
	switch {
	case req.Provider == "test.echo" && req.Model == "echo-v1":
		expected = Input
	case req.Provider == "test.digest" && req.Model == "sha256-v1":
		expected = fmt.Sprintf("%x", sha256.Sum256([]byte(Input)))
	default:
		return state.Operation{}, execution.ErrUnsupported
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(out.Output)))
	if out.Output != expected || out.Report.Digest != digest || out.Report.Units != len(Input) {
		return state.Operation{}, execution.ErrInvalid
	}
	op, err := s.GetOperation(ctx, req.OperationID)
	if err != nil {
		return state.Operation{}, err
	}
	a, err := s.GetAttempt(ctx, req.AttemptID)
	if err != nil {
		return state.Operation{}, err
	}
	if op.Descriptor.TargetRef != Target || op.Descriptor.Name != "analyze" || op.Descriptor.EffectClass != protocol.EffectReadOnly || op.Descriptor.VerificationPolicyRef != Policy || a.OperationID != op.Descriptor.ID || a.LeaseID != req.LeaseID || a.LeaseGeneration != op.LeaseGeneration || verifier == a.OwnerActorID {
		return state.Operation{}, core.ErrEvidenceScope
	}
	if op.State != state.OperationSucceeded && (op.State != state.OperationVerifying || op.ActiveAttemptID != req.AttemptID) {
		return state.Operation{}, core.ErrStaleOwnership
	}
	now := time.Now().UTC()
	id := string(req.AttemptID) + "/reference-verification"
	env := protocol.Envelope{ProtocolVersion: protocol.ProtocolVersion, MessageID: protocol.MessageID(id), MessageType: protocol.MessageObservation, MessageName: "verification.completed", TaskID: op.Descriptor.TaskID, OperationID: op.Descriptor.ID, AttemptID: req.AttemptID, CorrelationID: req.CorrelationID, CausationID: req.MessageID, Actor: protocol.Actor{ID: verifier, Role: "verifier"}, IssuedAt: out.Report.At, Payload: map[string]any{"digest": digest, "policy": Policy}}
	_, err = s.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
		record := evidence.Record{ID: protocol.EvidenceID(id), TaskID: op.Descriptor.TaskID, OperationID: op.Descriptor.ID, AttemptID: req.AttemptID, Kind: evidence.KindTargetObservation, SourceActorID: verifier, ObservedAt: now, SubjectRef: Target, Summary: "reference bytes matched", Digest: digest}
		if err := tx.RegisterEvidence(record); err != nil {
			return err
		}
		_, err := tx.ApplyVerification(op.Descriptor.ID, op.Revision, evidence.Verification{ID: id, OperationID: op.Descriptor.ID, AttemptID: req.AttemptID, PolicyRef: Policy, EvidenceIDs: []protocol.EvidenceID{record.ID}, Verdict: evidence.VerdictSatisfied, ActualStateSummary: "fixed synthetic result independently matched", VerifiedAt: now, VerifierActorID: verifier}, now)
		return err
	})
	if err != nil {
		return state.Operation{}, err
	}
	return s.GetOperation(ctx, op.Descriptor.ID)
}
