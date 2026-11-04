// Package reviewproof exercises the recovery boundary of a synthetic R6 write.
// The second store is a test fixture, not a ScopedKB transport.
package reviewproof

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

const (
	taskID    = "review-proof"
	opID      = "review-write"
	attemptID = "review-attempt"
	policy    = "synthetic-review-readback/v1"
	writer    = "fixture-writer"
	verifier  = "fixture-verifier"
)

type intent struct {
	Version      int
	Target       string
	BaseDigest   string
	ResultDigest string
}

func digest(raw []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	must(t, err)
	return raw
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func expected(t *testing.T) (intent, []byte) {
	t.Helper()
	payload := read(t, "testdata/candidate.json")
	return intent{1, "scopedkb-fixture/fact.example.review-windows-home",
		digest(read(t, "testdata/base.json")), digest(payload)}, payload
}

func intentDigest(t *testing.T, i intent) string {
	t.Helper()
	raw, err := json.Marshal(i)
	must(t, err)
	return digest(raw)
}

func resultPath(t *testing.T, root string, i intent) string {
	t.Helper()
	key := intentDigest(t, i)
	return filepath.Join(root, "knowledge", key[len("sha256:"):]+".json")
}

func openStore(t *testing.T, ctx context.Context, root string) *core.Store {
	t.Helper()
	repository, err := sqlite.Open(ctx, filepath.Join(root, "acp.sqlite"), sqlite.Options{})
	must(t, err)
	return core.NewWithRepository(repository)
}

func envelope(id, name, actor string, now time.Time) protocol.Envelope {
	return protocol.Envelope{ProtocolVersion: protocol.ProtocolVersion,
		MessageID: protocol.MessageID(id), MessageType: protocol.MessageObservation,
		MessageName: name, TaskID: taskID, OperationID: opID, CorrelationID: "review-correlation",
		Actor: protocol.Actor{ID: protocol.ActorID(actor), Role: "fixture"}, IssuedAt: now}
}

func begin(t *testing.T, ctx context.Context, store *core.Store, i intent, interrupt error) error {
	t.Helper()
	now := time.Now().UTC()
	expectedDigest := intentDigest(t, i)
	env := envelope("review-intent", "operation.offer", writer, now)
	env.MessageType = protocol.MessageCommand
	_, err := store.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
		if _, err := tx.CreateTask(taskID, now); err != nil {
			return err
		}
		op, err := tx.CreateOperation(protocol.OperationDescriptor{
			ID: opID, TaskID: taskID, Name: "knowledge.fixture.write", TargetRef: i.Target,
			EffectClass: protocol.EffectMutation, ConflictScope: []string{i.Target},
			IdempotencyMode:        protocol.IdempotencyConditional,
			AuthorizationPolicyRef: "operator-fixture/v1", VerificationPolicyRef: policy,
		}, now)
		if err != nil {
			return err
		}
		if err := tx.RegisterEvidence(evidence.Record{
			ID: "intent", TaskID: taskID, OperationID: opID, Kind: evidence.KindPolicyDecision,
			SourceActorID: writer, SubjectRef: i.Target, ObservedAt: now,
			Summary: "Синтетическое намерение записи с ожидаемой редакцией", Digest: expectedDigest,
		}); err != nil {
			return err
		}
		op, err = tx.MarkReady(opID, op.Revision, now)
		if err != nil {
			return err
		}
		if _, _, err := tx.StartAttempt(opID, op.Revision, attemptID, "review-lease", writer, "", now); err != nil {
			return err
		}
		return interrupt
	})
	return err
}

// TestWriterProcess exits after sync but before any acknowledgment to ACP.
// The parent waits for termination before exercising revocation.
func TestWriterProcess(t *testing.T) {
	root := os.Getenv("ACP_REVIEW_PROOF_ROOT")
	if root == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	i, payload := expected(t)
	store := openStore(t, ctx, root)
	defer store.Close()

	mode := os.Getenv("ACP_REVIEW_PROOF_MODE")
	if mode == "rollback" {
		interrupted := errors.New("synthetic failure before commit")
		if err := begin(t, ctx, store, i, interrupted); !errors.Is(err, interrupted) {
			t.Fatalf("unexpected transaction result: %v", err)
		}
		return
	}
	must(t, begin(t, ctx, store, i, nil))
	if mode == "absent" {
		os.Exit(42)
	}
	if digest(read(t, filepath.Join(root, "knowledge", "base.json"))) != i.BaseDigest {
		t.Fatal("stale expected revision")
	}
	if mode == "conflict" {
		payload = []byte("{}\n")
	}
	f, err := os.OpenFile(resultPath(t, root, i), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	must(t, err)
	_, err = f.Write(payload)
	must(t, err)
	must(t, f.Sync())
	must(t, f.Close())
	os.Exit(42)
}

func restore(t *testing.T, ctx context.Context, store *core.Store, root string, i intent) state.Operation {
	t.Helper()
	op, err := store.GetOperation(ctx, opID)
	must(t, err)
	if op.State == state.OperationSucceeded {
		return op
	}
	if (op.State != state.OperationExecuting && op.State != state.OperationUnknownOutcome) ||
		op.ActiveAttemptID != attemptID ||
		op.Descriptor.TargetRef != i.Target || op.Descriptor.VerificationPolicyRef != policy {
		t.Fatalf("unexpected recovered operation: %+v", op)
	}
	records, err := store.EvidenceForOperation(ctx, opID)
	must(t, err)
	bound := false
	for _, record := range records {
		if record.ID == "intent" && record.Digest == intentDigest(t, i) &&
			record.Kind == evidence.KindPolicyDecision && record.SourceActorID == writer &&
			record.SubjectRef == i.Target {
			bound = true
		}
	}
	if !bound {
		t.Fatal("missing exact persisted intent")
	}
	now := time.Now().UTC()
	if op.State == state.OperationExecuting {
		op, err = store.InvalidateAttempt(ctx, opID, op.Revision, attemptID, core.ControllerRestart, evidence.Record{
			ID: "lost", TaskID: taskID, OperationID: opID, AttemptID: attemptID,
			Kind: evidence.KindProcessObservation, SourceActorID: verifier, ObservedAt: now,
			SubjectRef: i.Target, Summary: "Процесс завершился до сообщения о результате",
		}, now)
		must(t, err)
	}
	if op.State != state.OperationUnknownOutcome {
		t.Fatalf("unexpected state after revocation: %s", op.State)
	}
	actual, err := os.ReadFile(resultPath(t, root, i))
	base, baseErr := os.ReadFile(filepath.Join(root, "knowledge", "base.json"))
	if err != nil || baseErr != nil || digest(actual) != i.ResultDigest || digest(base) != i.BaseDigest {
		return op
	}
	env := envelope("review-confirmed", "verification.completed", verifier, now)
	env.AttemptID = attemptID
	_, err = store.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
		current, err := tx.BeginVerification(opID, op.Revision, now)
		if err != nil {
			return err
		}
		observation := evidence.Record{
			ID: "readback", TaskID: taskID, OperationID: opID, AttemptID: attemptID,
			Kind: evidence.KindTargetObservation, SourceActorID: verifier, ObservedAt: now,
			SubjectRef: i.Target, Summary: "Совпали ожидаемая редакция и байты кандидата",
			Digest: digest(actual),
		}
		if err := tx.RegisterEvidence(observation); err != nil {
			return err
		}
		_, err = tx.ApplyVerification(opID, current.Revision, evidence.Verification{
			ID: "review-verification", OperationID: opID, AttemptID: attemptID,
			PolicyRef: policy, EvidenceIDs: []protocol.EvidenceID{observation.ID},
			Verdict: evidence.VerdictSatisfied, ActualStateSummary: observation.Summary,
			VerifiedAt: now, VerifierActorID: verifier,
		}, now)
		return err
	})
	must(t, err)
	task, err := store.GetTask(ctx, taskID)
	must(t, err)
	task, err = store.FinalizeTask(ctx, taskID, task.Revision, now)
	must(t, err)
	if task.State != state.TaskCompleted {
		t.Fatalf("task state=%s", task.State)
	}
	op, err = store.GetOperation(ctx, opID)
	must(t, err)
	return op
}

func TestReviewWriteRecoveryAfterProcessExit(t *testing.T) {
	for _, mode := range []string{"saved", "absent", "conflict", "stale-base", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			i, _ := expected(t)
			knowledge := filepath.Join(root, "knowledge")
			must(t, os.Mkdir(knowledge, 0700))
			must(t, os.WriteFile(filepath.Join(knowledge, "base.json"), read(t, "testdata/base.json"), 0600))

			executable, err := os.Executable()
			must(t, err)
			child := exec.CommandContext(ctx, executable, "-test.run=^TestWriterProcess$")
			child.Env = append(os.Environ(), "ACP_REVIEW_PROOF_ROOT="+root, "ACP_REVIEW_PROOF_MODE="+mode)
			out, err := child.CombinedOutput()
			if mode == "rollback" {
				must(t, err)
			} else {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 42 {
					t.Fatalf("writer exit=%v output=%s", err, out)
				}
			}

			store := openStore(t, ctx, root)
			defer store.Close()
			path := resultPath(t, root, i)
			if mode == "rollback" {
				if _, err := store.GetOperation(ctx, opID); !errors.Is(err, core.ErrNotFound) {
					t.Fatalf("operation escaped rollback: %v", err)
				}
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("external effect escaped rollback: %v", err)
				}
				return
			}

			before, readErr := os.ReadFile(path)
			if mode == "absent" {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatalf("expected absent candidate, got %v", readErr)
				}
			} else {
				must(t, readErr)
			}
			if mode == "stale-base" {
				must(t, os.WriteFile(filepath.Join(knowledge, "base.json"), []byte("{}"), 0600))
			}
			op := restore(t, ctx, store, root, i)
			want := state.OperationUnknownOutcome
			if mode == "saved" {
				want = state.OperationSucceeded
			}
			if op.State != want || op.LeaseGeneration != 1 {
				t.Fatalf("recovered=%s generation=%d", op.State, op.LeaseGeneration)
			}
			again := restore(t, ctx, store, root, i)
			if again.Revision != op.Revision || again.State != want {
				t.Fatalf("replay changed recovered operation: %+v", again)
			}
			if err := store.CheckActiveAttemptContext(ctx, opID, attemptID, "review-lease", 1, writer); !errors.Is(err, core.ErrStaleOwnership) {
				t.Fatalf("old authority returned: %v", err)
			}
			after, readErr := os.ReadFile(path)
			if mode == "absent" {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatalf("candidate created during recovery: %v", readErr)
				}
			} else {
				must(t, readErr)
				if string(after) != string(before) {
					t.Fatal("candidate was rewritten")
				}
			}
			entries, err := os.ReadDir(knowledge)
			must(t, err)
			wantFiles := 2
			if mode == "absent" {
				wantFiles = 1
			}
			if len(entries) != wantFiles {
				t.Fatalf("knowledge files=%d", len(entries))
			}
		})
	}
}
