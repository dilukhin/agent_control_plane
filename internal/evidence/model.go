package evidence

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

type Kind string

const (
	KindDeliveryReceipt      Kind = "delivery_receipt"
	KindWorkerReport         Kind = "worker_report"
	KindProviderReport       Kind = "provider_report"
	KindTransportObservation Kind = "transport_observation"
	KindTargetObservation    Kind = "target_observation"
	KindProcessObservation   Kind = "process_observation"
	KindArtifactObservation  Kind = "artifact_observation"
	KindVerificationResult   Kind = "verification_result"
	KindUserDecision         Kind = "user_decision"
	KindPolicyDecision       Kind = "policy_decision"
)

type Verdict string

const (
	VerdictSatisfied             Verdict = "satisfied"
	VerdictNotSatisfiedRetryable Verdict = "not_satisfied_retryable"
	VerdictNotSatisfiedTerminal  Verdict = "not_satisfied_terminal"
	VerdictInconclusive          Verdict = "inconclusive"
)

type Record struct {
	ID            protocol.EvidenceID
	TaskID        protocol.TaskID
	OperationID   protocol.OperationID
	AttemptID     protocol.AttemptID
	Kind          Kind
	SourceActorID protocol.ActorID
	ObservedAt    time.Time
	SubjectRef    string
	Summary       string
	ArtifactRef   string
	Digest        string
}

func (r Record) Validate() error {
	if r.ID == "" || r.TaskID == "" || r.OperationID == "" || r.SourceActorID == "" {
		return errors.New("evidence identity, task, operation, and source are required")
	}
	if r.ObservedAt.IsZero() || strings.TrimSpace(r.SubjectRef) == "" {
		return errors.New("observed_at and subject_ref are required")
	}
	switch r.Kind {
	case KindDeliveryReceipt, KindWorkerReport, KindProviderReport, KindTransportObservation,
		KindTargetObservation, KindProcessObservation, KindArtifactObservation, KindVerificationResult,
		KindUserDecision, KindPolicyDecision:
		return nil
	default:
		return fmt.Errorf("unknown evidence kind %q", r.Kind)
	}
}

type Verification struct {
	ID                 string
	OperationID        protocol.OperationID
	AttemptID          protocol.AttemptID
	PolicyRef          string
	EvidenceIDs        []protocol.EvidenceID
	Verdict            Verdict
	RetryExecutionSafe bool
	ActualStateSummary string
	VerifiedAt         time.Time
	VerifierActorID    protocol.ActorID
}

func (v Verification) Validate() error {
	if strings.TrimSpace(v.ID) == "" || v.OperationID == "" || strings.TrimSpace(v.PolicyRef) == "" || v.VerifierActorID == "" {
		return errors.New("verification identity, operation, policy, and verifier are required")
	}
	if v.VerifiedAt.IsZero() || len(v.EvidenceIDs) == 0 {
		return errors.New("verification timestamp and evidence are required")
	}
	switch v.Verdict {
	case VerdictSatisfied, VerdictNotSatisfiedTerminal, VerdictInconclusive:
		return nil
	case VerdictNotSatisfiedRetryable:
		if !v.RetryExecutionSafe {
			return errors.New("retryable verdict requires execution safety confirmation")
		}
		return nil
	default:
		return fmt.Errorf("unknown verification verdict %q", v.Verdict)
	}
}
