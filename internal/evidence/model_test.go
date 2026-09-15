package evidence

import (
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

func TestRetryableVerdictRequiresExecutionSafety(t *testing.T) {
	v := Verification{
		ID:              "ver_1",
		OperationID:     "op_1",
		PolicyRef:       "verify:v1",
		EvidenceIDs:     []protocol.EvidenceID{"ev_1"},
		Verdict:         VerdictNotSatisfiedRetryable,
		VerifiedAt:      time.Now(),
		VerifierActorID: "actor_1",
	}
	if err := v.Validate(); err == nil {
		t.Fatal("expected retry safety validation error")
	}
	v.RetryExecutionSafe = true
	if err := v.Validate(); err != nil {
		t.Fatalf("safe retry verdict rejected: %v", err)
	}
}
