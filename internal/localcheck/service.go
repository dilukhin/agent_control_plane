package localcheck

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/execution"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var ErrExisting = errors.New("задание уже существует: используйте status или recover; повторный запуск запрещён")
var ErrReceipt = errors.New("нет достаточной сохранённой квитанции завершения; исход требует отдельной проверки")

const marker = "ACP trusted local checks v1\n"
const verifyPolicy = "local-process-exit/v1"

type Result struct {
	Task        string               `json:"task"`
	TaskState   state.TaskState      `json:"task_state"`
	Operation   state.OperationState `json:"operation_state"`
	ExitCode    *int                 `json:"exit_code,omitempty"`
	Bytes       int                  `json:"output_bytes,omitempty"`
	Digest      string               `json:"output_sha256,omitempty"`
	InputsMatch *bool                `json:"inputs_match,omitempty"`
}

func operationID(id string) protocol.OperationID { return protocol.OperationID("op_" + id) }
func target(id string) string                    { return "local-check/" + id }

func open(ctx context.Context, root string, create bool) (*core.Store, string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, "", ErrConfig
	}
	if create {
		if err := os.MkdirAll(abs, 0700); err != nil {
			return nil, "", ErrConfig
		}
	}
	info, err := os.Lstat(abs)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, "", ErrConfig
	}
	mark := filepath.Join(abs, "owner.txt")
	if _, err := regular(mark); err != nil {
		items, e := os.ReadDir(abs)
		if !create || e != nil || len(items) != 0 {
			return nil, "", ErrConfig
		}
		f, e := os.OpenFile(mark, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return nil, "", ErrConfig
		}
		_, e = f.WriteString(marker)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e != nil || ce != nil {
			return nil, "", ErrConfig
		}
	}
	b, err := os.ReadFile(mark)
	if err != nil || string(b) != marker {
		return nil, "", ErrConfig
	}
	dir := filepath.Join(abs, "receipts")
	if create {
		if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, "", ErrConfig
		}
	}
	info, err = os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, "", ErrConfig
	}
	if !create {
		if _, err := regular(filepath.Join(abs, "state.sqlite")); err != nil {
			return nil, "", ErrConfig
		}
	}
	r, err := sqlite.Open(ctx, filepath.Join(abs, "state.sqlite"), sqlite.Options{})
	if err != nil {
		return nil, "", err
	}
	return core.NewWithRepository(r), abs, nil
}

func receiptPath(root, id string) string { return filepath.Join(root, "receipts", id+".json") }

func saveReceipt(root, id string, r Receipt) error {
	b, err := json.Marshal(r)
	if err != nil || len(b) > 4096 {
		return ErrReceipt
	}
	// Never replace a previous receipt, including a partial write after a crash.
	f, err := os.OpenFile(receiptPath(root, id), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ErrReceipt
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil || ce != nil {
		return ErrReceipt
	}
	return nil
}

func loadReceipt(root, id string) (Receipt, error) {
	var r Receipt
	s, err := regular(receiptPath(root, id))
	if err != nil || s.Size() > 4096 {
		return r, ErrReceipt
	}
	f, err := os.Open(receiptPath(root, id))
	if err != nil {
		return r, ErrReceipt
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(b) > 4096 || decode(b, &r) != nil || r.Version != 1 || string(r.Task) != id || r.Operation != operationID(id) || r.Attempt != protocol.AttemptID("att_"+id) || r.Lease != protocol.LeaseID("lease_"+id) || r.Generation != 1 || !validDigest(r.Config) || !validDigest(r.OutputHash) || r.OutputBytes < 0 || r.OutputBytes > 64*1024 || r.ExitCode < 0 || r.Finished.IsZero() || r.Finished.After(time.Now().Add(time.Second)) {
		return Receipt{}, ErrReceipt
	}
	return r, nil
}

// Check runs one explicit local command. Tests may write files, so the operation
// is conservatively a mutation reserving the canonical workspace, not read_only.
func Check(ctx context.Context, root, id string, config Config) (Result, []byte, error) {
	if !idPattern.MatchString(id) || !validDigest(config.Digest) {
		return Result{}, nil, ErrConfig
	}
	config = config.clone()
	if err := config.CheckInputs(); err != nil {
		return Result{}, nil, err
	}
	s, abs, err := open(ctx, root, true)
	if err != nil {
		return Result{}, nil, err
	}
	defer s.Close()
	if _, err := s.GetTask(ctx, protocol.TaskID(id)); err == nil {
		return Result{}, nil, ErrExisting
	} else if !errors.Is(err, core.ErrNotFound) {
		return Result{}, nil, err
	}
	now := time.Now().UTC()
	_, err = s.CreateTask(ctx, protocol.TaskID(id), now)
	if errors.Is(err, p.ErrDuplicateID) {
		return Result{}, nil, ErrExisting
	}
	if err != nil {
		return Result{}, nil, err
	}
	d := protocol.OperationDescriptor{ID: operationID(id), TaskID: protocol.TaskID(id), Name: "project.check", TargetRef: target(id), EffectClass: protocol.EffectMutation, ConflictScope: []string{"local-workspace/" + sum([]byte(config.Workspace))}, IdempotencyMode: protocol.IdempotencyUnknown, VerificationPolicyRef: verifyPolicy, AuthorizationPolicyRef: "operator-local-check/v1"}
	op, err := s.CreateOperation(ctx, d, now)
	if err != nil {
		return Result{}, nil, err
	}
	intent := evidence.Record{ID: protocol.EvidenceID("intent_" + id), TaskID: d.TaskID, OperationID: d.ID, Kind: evidence.KindPolicyDecision, SourceActorID: controllerActor, ObservedAt: now, SubjectRef: d.TargetRef, Summary: "operator-selected local check", Digest: "sha256:" + config.Digest}
	if err := s.RegisterEvidence(ctx, intent); err != nil {
		return Result{}, nil, err
	}
	op, err = s.MarkReady(ctx, d.ID, op.Revision, now)
	if err != nil {
		return Result{}, nil, err
	}
	worker := &commandWorker{config: config}
	limits := worker.Capabilities().Limits
	deadline := now.Add(config.duration())
	policy := execution.NewExactPolicy([]execution.Grant{{PolicyRef: d.AuthorizationPolicyRef, Worker: workerActor, Provider: "local.command", Model: "process-v1", Action: d.Name, Target: d.TargetRef, DataClass: "local-project", Effect: d.EffectClass, Expires: deadline, Limits: limits}})
	runner, err := execution.NewRunner(s, controllerActor, worker, policy)
	if err != nil {
		return Result{}, nil, err
	}
	req := execution.Request{Version: execution.Version, MessageID: protocol.MessageID("offer_" + id), OperationID: d.ID, AttemptID: protocol.AttemptID("att_" + id), LeaseID: protocol.LeaseID("lease_" + id), CorrelationID: protocol.CorrelationID("corr_" + id), Revision: op.Revision, IssuedAt: now, Deadline: deadline, Provider: "local.command", Model: "process-v1", DataClass: "local-project", Input: config.Digest, Limits: limits}
	out, err := runner.Execute(ctx, req)
	// No provider call or retry after any ambiguous persistence error.
	if err != nil {
		return Result{Task: id}, nil, err
	}
	if out.Disposition != execution.Reported || !out.Applied {
		result, e := status(ctx, s, abs, id)
		if e != nil {
			return result, nil, e
		}
		return result, nil, ErrReceipt
	}
	worker.mu.Lock()
	r, output := worker.receipt, append([]byte{}, worker.output...)
	worker.mu.Unlock()
	if r == nil {
		return Result{Task: id}, nil, ErrReceipt
	}
	claim, _ := json.Marshal(r)
	if out.Output != string(claim) || out.Report.Digest != "sha256:"+sum(claim) || r.OutputHash != sum(output) {
		return Result{Task: id}, nil, ErrReceipt
	}
	if err := saveReceipt(abs, id, *r); err != nil {
		return Result{Task: id}, nil, err
	}
	if err := verify(ctx, s, abs, id); err != nil {
		return Result{Task: id}, nil, err
	}
	result, err := status(ctx, s, abs, id)
	return result, output, err
}

func verify(ctx context.Context, s *core.Store, root, id string) error {
	r, err := loadReceipt(root, id)
	if err != nil {
		return err
	}
	op, err := s.GetOperation(ctx, operationID(id))
	if err != nil {
		return err
	}
	if op.State == state.OperationSucceeded || op.State == state.OperationFailed {
		return finalize(ctx, s, id)
	}
	if op.State != state.OperationVerifying || op.Descriptor.TargetRef != target(id) || op.Descriptor.VerificationPolicyRef != verifyPolicy || op.ActiveAttemptID != r.Attempt {
		return ErrReceipt
	}
	a, err := s.GetAttempt(ctx, r.Attempt)
	if err != nil || a.OwnerActorID != workerActor || a.LeaseID != r.Lease || a.LeaseGeneration != r.Generation {
		return ErrReceipt
	}
	evs, err := s.EvidenceForOperation(ctx, op.Descriptor.ID)
	if err != nil {
		return err
	}
	foundIntent, foundReport := false, false
	b, _ := json.Marshal(r)
	for _, e := range evs {
		if e.ID == protocol.EvidenceID("intent_"+id) && e.SourceActorID == controllerActor && e.Digest == "sha256:"+r.Config {
			foundIntent = true
		}
		if e.Kind == evidence.KindWorkerReport && e.AttemptID == r.Attempt && e.SourceActorID == workerActor && e.Digest == "sha256:"+sum(b) {
			foundReport = true
		}
	}
	if !foundIntent || !foundReport {
		return ErrReceipt
	}
	now := time.Now().UTC()
	verdict := evidence.VerdictSatisfied
	summary := "selected command exited zero; selected source bytes matched"
	if r.ExitCode != 0 || !r.InputsMatch {
		verdict = evidence.VerdictNotSatisfiedTerminal
		summary = "selected command failed or selected source bytes changed"
	}
	env := protocol.Envelope{ProtocolVersion: protocol.ProtocolVersion, MessageID: protocol.MessageID("verified_" + id), MessageType: protocol.MessageObservation, MessageName: "verification.completed", TaskID: protocol.TaskID(id), OperationID: op.Descriptor.ID, AttemptID: r.Attempt, CorrelationID: protocol.CorrelationID("corr_" + id), Actor: protocol.Actor{ID: controllerActor, Role: "verifier"}, IssuedAt: r.Finished, Payload: map[string]any{"receipt_digest": sum(b)}}
	_, err = s.ProcessMessage(ctx, env, func(tx *core.Transaction) error {
		ev := evidence.Record{ID: protocol.EvidenceID("observed_" + id), TaskID: protocol.TaskID(id), OperationID: op.Descriptor.ID, AttemptID: r.Attempt, Kind: evidence.KindProcessObservation, SourceActorID: controllerActor, ObservedAt: now, SubjectRef: target(id), Summary: summary, Digest: "sha256:" + sum(b), ArtifactRef: "receipts/" + id + ".json"}
		if err := tx.RegisterEvidence(ev); err != nil {
			return err
		}
		_, err := tx.ApplyVerification(op.Descriptor.ID, op.Revision, evidence.Verification{ID: "verification_" + id, OperationID: op.Descriptor.ID, AttemptID: r.Attempt, PolicyRef: verifyPolicy, EvidenceIDs: []protocol.EvidenceID{ev.ID}, Verdict: verdict, ActualStateSummary: summary, VerifiedAt: now, VerifierActorID: controllerActor}, now)
		return err
	})
	if err != nil {
		return err
	}
	return finalize(ctx, s, id)
}

func finalize(ctx context.Context, s *core.Store, id string) error {
	task, err := s.GetTask(ctx, protocol.TaskID(id))
	if err != nil {
		return err
	}
	if task.State == state.TaskCompleted || task.State == state.TaskFailed || task.State == state.TaskCancelled {
		return nil
	}
	_, err = s.FinalizeTask(ctx, task.ID, task.Revision, time.Now().UTC())
	return err
}

func status(ctx context.Context, s *core.Store, root, id string) (Result, error) {
	task, err := s.GetTask(ctx, protocol.TaskID(id))
	if err != nil {
		return Result{}, err
	}
	op, err := s.GetOperation(ctx, operationID(id))
	if err != nil {
		return Result{Task: id, TaskState: task.State}, err
	}
	result := Result{Task: id, TaskState: task.State, Operation: op.State}
	if op.State == state.OperationSucceeded || op.State == state.OperationFailed {
		r, err := loadReceipt(root, id)
		if err != nil {
			return result, err
		}
		b, _ := json.Marshal(r)
		evs, err := s.EvidenceForOperation(ctx, operationID(id))
		if err != nil {
			return result, err
		}
		matched := false
		for _, ev := range evs {
			if ev.ID == protocol.EvidenceID("observed_"+id) && ev.SourceActorID == controllerActor && ev.Digest == "sha256:"+sum(b) {
				matched = true
			}
		}
		if !matched {
			return result, ErrReceipt
		}
		result.ExitCode, result.InputsMatch, result.Bytes, result.Digest = &r.ExitCode, &r.InputsMatch, r.OutputBytes, r.OutputHash
	}
	return result, nil
}

// Status reads canonical state. Opening SQLite may perform its normal journal
// maintenance, but never runs a command or infers success from a receipt alone.
func Status(ctx context.Context, root, id string) (Result, error) {
	if !idPattern.MatchString(id) {
		return Result{}, ErrConfig
	}
	s, abs, err := open(ctx, root, false)
	if err != nil {
		return Result{}, err
	}
	defer s.Close()
	return status(ctx, s, abs, id)
}

// Recover only verifies an already reported matching completion. It never
// launches a process, clears unknown outcome, or reconstructs output from hash.
func Recover(ctx context.Context, root, id string) (Result, error) {
	if !idPattern.MatchString(id) {
		return Result{}, ErrConfig
	}
	s, abs, err := open(ctx, root, false)
	if err != nil {
		return Result{}, err
	}
	defer s.Close()
	if err := verify(ctx, s, abs, id); err != nil {
		return Result{Task: id}, err
	}
	return status(ctx, s, abs, id)
}

// Collect uses R3 retention, then optionally deletes one old owned receipt whose
// canonical task no longer exists. Unknown/active tasks and foreign files stay.
type Collection struct {
	State           p.GCResult `json:"state"`
	RemovedReceipts int        `json:"removed_receipts"`
}

func Collect(ctx context.Context, root, after, receipt string) (Collection, error) {
	var result Collection
	if (after != "" && !idPattern.MatchString(after)) || (receipt != "" && !idPattern.MatchString(receipt)) {
		return result, ErrConfig
	}
	s, abs, err := open(ctx, root, false)
	if err != nil {
		return result, err
	}
	defer s.Close()
	now := time.Now().UTC()
	result.State, err = s.Collect(ctx, p.GCOptions{Now: now, After: protocol.TaskID(after), MaxTasks: 25, MaxRows: 1000})
	if err != nil {
		return result, err
	}
	// Explicit ID keeps receipt cleanup bounded without an unbounded directory
	// scan or starvation behind the first page of retained receipts.
	if receipt != "" {
		r, err := loadReceipt(abs, receipt)
		if err != nil {
			return result, err
		}
		_, e := s.GetTask(ctx, protocol.TaskID(receipt))
		if e != nil && !errors.Is(e, core.ErrNotFound) {
			return result, e
		}
		if errors.Is(e, core.ErrNotFound) && r.Finished.Before(now.Add(-30*24*time.Hour)) {
			if err := os.Remove(receiptPath(abs, receipt)); err != nil {
				return result, ErrReceipt
			}
			result.RemovedReceipts = 1
		}
	}
	return result, nil
}
