package process

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/reference"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var workerBinary string
var bg = context.Background()

func TestMain(m *testing.M) {
	// Fault children use this same test executable, but do not recursively run
	// tests or build another executable. Positive E2E uses the production CLI.
	if len(os.Args) > 2 && os.Args[1] == "--r5-fault" {
		os.Exit(faultChild(os.Args[2]))
	}
	root, err := os.MkdirTemp("", "acp-r5-process-tests-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "test directory unavailable")
		os.Exit(1)
	}
	workerBinary = filepath.Join(root, "acp-reference-worker")
	if runtime.GOOS == "windows" {
		workerBinary += ".exe"
	}
	_, file, _, _ := runtime.Caller(0)
	ctx, cancel := context.WithTimeout(bg, 2*time.Minute)
	goExe := filepath.Join(runtime.GOROOT(), "bin", "go")
	if runtime.GOOS == "windows" {
		goExe += ".exe"
	}
	build := exec.CommandContext(ctx, goExe, "build", "-buildvcs=false", "-o", workerBinary, "./cmd/acp-reference-worker")
	build.Dir = filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	output, err := build.CombinedOutput()
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build reference worker: %v\n%s", err, output)
		os.RemoveAll(root)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(root) // only the private directory created by this TestMain
	os.Exit(code)
}

func faultChild(mode string) int {
	if mode == "serve" {
		if err := Serve(bg, os.Stdin, os.Stdout, protocol.ActorID(os.Args[len(os.Args)-2]), os.Args[len(os.Args)-1]); err != nil {
			return 1
		}
		return 0
	}
	if mode == "before-read" {
		return 7
	}
	f, err := readFrame(os.Stdin)
	if err != nil || f.Job == nil {
		return 8
	}
	j := *f.Job
	hb := frame{Version: WireVersion, Kind: "heartbeat", Binding: bind(j), PID: os.Getpid()}
	if writeFrame(os.Stdout, hb) != nil {
		return 8
	}
	ack, err := readFrame(os.Stdin)
	if err != nil || ack.Kind != "continue" {
		return 8
	}
	if mode == "block" {
		for {
			time.Sleep(time.Hour)
		}
	}
	if mode == "disconnect" {
		os.Stdout.Close()
		return 0
	}
	if mode == "crash" {
		return 7
	}
	if mode == "stderr" {
		os.Stderr.Write([]byte(strings.Repeat("x", MaxStderr+100)))
		for {
			time.Sleep(time.Hour)
		}
	}
	if mode == "oversize-header" {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], MaxFrame+1)
		os.Stdout.Write(h[:])
		return 0
	}
	if mode == "truncated" {
		os.Stdout.Write([]byte{0, 0, 0, 20, '{'})
		return 0
	}
	if mode == "heartbeat-flood" {
		for i := 0; i < execution.MaxHeartbeats+1; i++ {
			if writeFrame(os.Stdout, hb) != nil {
				return 8
			}
			if _, err := readFrame(os.Stdin); err != nil {
				return 8
			}
		}
		return 8
	}
	p, _ := provider(j.Request.Provider)
	claim, err := p.Generate(bg, execution.ProviderRequest{Model: j.Request.Model, Input: j.Request.Input, Limits: j.Request.Limits})
	if err != nil {
		return 9
	}
	result := frame{Version: WireVersion, Kind: "result", Binding: bind(j), Claim: &claim, PID: os.Getpid()}
	switch mode {
	case "version":
		result.Version = "9"
	case "attempt":
		result.Binding.Attempt = "foreign"
	case "generation":
		result.Binding.Generation++
	case "correlation":
		result.Binding.Correlation = "foreign"
	case "worker":
		result.Binding.Worker = "foreign"
	case "target-operation":
		result.Binding.Operation = "foreign"
	case "pid":
		result.PID++
	case "units":
		result.Claim.Units = j.Request.Limits.Units + 1
	case "output":
		result.Claim.Output = strings.Repeat("x", j.Request.Limits.OutputBytes+1)
	case "wrong-output":
		result.Claim.Output = "wrong but well-formed result"
	case "raw-error":
		result.Kind = "error"
		result.Claim = nil
		result.Reason = "test-only-secret-must-not-persist"
	case "unknown-kind":
		result.Kind = "approve"
	case "environment":
		if _, ok := os.LookupEnv("ACP_R5_TEST_SECRET"); ok {
			return 10
		}
	}
	if writeFrame(os.Stdout, result) != nil {
		return 8
	}
	if mode == "duplicate" {
		writeFrame(os.Stdout, result)
	}
	if mode == "garbage" {
		os.Stdout.Write([]byte("x"))
	}
	if mode == "result-nonzero-exit" {
		return 7
	}
	return 0
}

func newWorker(t *testing.T, providerID, fault string, capacity int) *Worker {
	t.Helper()
	c := Config{Executable: workerBinary, MaxParallel: capacity}
	if fault != "" {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		c.Executable = exe
		c.FixedArgs = []string{"--r5-fault", fault}
	}
	w, err := New(c, "process-worker", providerID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		if err := w.WaitIdle(ctx); err != nil {
			t.Error("owned process cleanup:", err)
		}
	})
	return w
}

func openSQLite(t *testing.T, path string) *core.Store {
	t.Helper()
	r, err := sqlite.Open(bg, path, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return core.NewWithRepository(r)
}

func withStores(t *testing.T, fn func(*testing.T, *core.Store)) {
	t.Helper()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			s := core.NewStore()
			if backend == "sqlite" {
				s = openSQLite(t, filepath.Join(t.TempDir(), "state.db"))
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			fn(t, s)
		})
	}
}

func scenario(t *testing.T, s *core.Store, w *Worker, id string) (*execution.Runner, execution.Request) {
	t.Helper()
	now := time.Now().UTC()
	task := protocol.TaskID("task-" + id)
	if _, err := s.CreateTask(bg, task, now); err != nil {
		t.Fatal(err)
	}
	d := protocol.OperationDescriptor{ID: protocol.OperationID("op-" + id), TaskID: task, Name: "analyze", TargetRef: reference.Target, EffectClass: protocol.EffectReadOnly, IdempotencyMode: protocol.IdempotencyIdempotent, VerificationPolicyRef: reference.Policy, AuthorizationPolicyRef: "allow-process/v1"}
	op, err := s.CreateOperation(bg, d, now)
	if err != nil {
		t.Fatal(err)
	}
	op, err = s.MarkReady(bg, d.ID, op.Revision, now)
	if err != nil {
		t.Fatal(err)
	}
	c := w.Capabilities()
	req := execution.Request{Version: execution.Version, MessageID: protocol.MessageID("offer-" + id), OperationID: d.ID, AttemptID: protocol.AttemptID("attempt-" + id), LeaseID: protocol.LeaseID("lease-" + id), CorrelationID: protocol.CorrelationID("chain-" + id), Revision: op.Revision, IssuedAt: now, Deadline: now.Add(15 * time.Second), Provider: c.Provider, Model: c.Models[0], DataClass: "synthetic", Input: reference.Input, Limits: execution.Limits{Duration: 15 * time.Second, InputBytes: 1024, OutputBytes: 1024, Units: 1024}}
	r := runnerFor(t, s, w, req)
	return r, req
}

func runnerFor(t *testing.T, s *core.Store, w *Worker, req execution.Request) *execution.Runner {
	t.Helper()
	g := execution.Grant{PolicyRef: "allow-process/v1", Worker: w.Capabilities().Worker, Provider: req.Provider, Model: req.Model, Action: "analyze", Target: reference.Target, DataClass: "synthetic", Effect: protocol.EffectReadOnly, Expires: time.Now().Add(time.Minute), Limits: req.Limits}
	r, err := execution.NewRunner(s, "controller", w, execution.NewExactPolicy([]execution.Grant{g}))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func operation(t *testing.T, s *core.Store, id protocol.OperationID) state.Operation {
	t.Helper()
	op, err := s.GetOperation(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func waitIdle(t *testing.T, w *Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	if err := w.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	stats := w.Snapshot()
	if stats.Active != 0 || stats.Started != stats.Reaped {
		t.Fatalf("unreaped processes: %+v", stats)
	}
}

func waitHeartbeats(t *testing.T, w *Worker, n uint64) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if w.Snapshot().Heartbeats >= n {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("no process handshake: %+v", w.Snapshot())
		}
	}
}

func TestRealWorkerCompleteVerifyAndReopen(t *testing.T) {
	for _, providerID := range []string{"test.echo", "test.digest"} {
		t.Run(providerID, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s := openSQLite(t, path)
			defer func() { s.Close() }()
			w := newWorker(t, providerID, "", 0)
			r, req := scenario(t, s, w, "success")
			out, err := r.Execute(bg, req)
			if err != nil || out.Disposition != execution.Reported || !out.Applied {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			waitIdle(t, w)
			if stats := w.Snapshot(); stats.Started != 1 || stats.LastPID == os.Getpid() || stats.LastPID <= 0 || stats.Heartbeats != 1 {
				t.Fatalf("not a distinct process: %+v", stats)
			}
			if operation(t, s, req.OperationID).State != state.OperationVerifying {
				t.Fatal("worker verified its own result")
			}
			task, err := s.GetTask(bg, operation(t, s, req.OperationID).Descriptor.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.FinalizeTask(bg, task.ID, task.Revision, time.Now().UTC()); err == nil {
				t.Fatal("unverified task finalized")
			}
			bad := out
			bad.Output = "forged"
			if _, err := reference.Verify(bg, s, req, bad, "verifier"); err == nil {
				t.Fatal("wrong output verified")
			}
			if _, err := reference.Verify(bg, s, req, out, w.Capabilities().Worker); err == nil {
				t.Fatal("worker actor self-verified")
			}
			before := operation(t, s, req.OperationID)
			if before.State != state.OperationVerifying {
				t.Fatal("negative verification changed state")
			}
			op, err := reference.Verify(bg, s, req, out, "verifier")
			if err != nil || op.State != state.OperationSucceeded {
				t.Fatalf("verify=%+v %v", op, err)
			}
			if replay, err := reference.Verify(bg, s, req, out, "verifier"); err != nil || replay.Revision != op.Revision {
				t.Fatalf("verification replay: %+v %v", replay, err)
			}
			if completed, err := s.FinalizeTask(bg, task.ID, task.Revision, time.Now().UTC()); err != nil || completed.State != state.TaskCompleted {
				t.Fatalf("task completion: %+v %v", completed, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openSQLite(t, path)
			fresh := newWorker(t, providerID, "", 0)
			r = runnerFor(t, s, fresh, req)
			dup, err := r.Execute(bg, req)
			if err != nil || dup.Disposition != execution.Duplicate || fresh.Snapshot().Started != 0 {
				t.Fatalf("reopen replay: %+v %v", dup, err)
			}
			if operation(t, s, req.OperationID).State != state.OperationSucceeded {
				t.Fatal("lost verified completion")
			}
			if completed, err := s.GetTask(bg, task.ID); err != nil || completed.State != state.TaskCompleted {
				t.Fatalf("lost terminal task: %+v %v", completed, err)
			}
		})
	}
}

func TestTransportFailuresNeverBecomeSuccessOrRetry(t *testing.T) {
	for _, fault := range []string{"before-read", "disconnect", "crash", "version", "attempt", "generation", "correlation", "worker", "target-operation", "pid", "units", "output", "raw-error", "unknown-kind", "duplicate", "garbage", "result-nonzero-exit", "oversize-header", "truncated", "heartbeat-flood", "stderr"} {
		t.Run(fault, func(t *testing.T) {
			withStores(t, func(t *testing.T, s *core.Store) {
				w := newWorker(t, "test.echo", fault, 0)
				r, req := scenario(t, s, w, "failure")
				out, err := r.Execute(bg, req)
				if err != nil || out.Disposition != execution.Unknown {
					t.Fatalf("out=%+v err=%v", out, err)
				}
				waitIdle(t, w)
				op := operation(t, s, req.OperationID)
				if op.State != state.OperationUnknownOutcome {
					t.Fatal("transport failure became known completion")
				}
				a, err := s.GetAttempt(bg, req.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.CheckActiveAttemptContext(bg, req.OperationID, a.ID, a.LeaseID, a.LeaseGeneration, a.OwnerActorID); !errors.Is(err, core.ErrStaleOwnership) {
					t.Fatalf("authority survived failure: %v", err)
				}
				out, err = r.Execute(bg, req)
				if err != nil || out.Disposition != execution.Duplicate || w.Snapshot().Started != 1 {
					t.Fatalf("automatic retry: %+v %v stats=%+v", out, err, w.Snapshot())
				}
				records, err := s.EvidenceForOperation(bg, req.OperationID)
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range records {
					if strings.Contains(rec.Summary, "test-only-secret") || strings.Contains(rec.Summary, reference.Input) {
						t.Fatal("raw payload leaked")
					}
				}
			})
		})
	}
}

func TestSyntacticallyValidFalseClaimNeedsVerifier(t *testing.T) {
	withStores(t, func(t *testing.T, s *core.Store) {
		w := newWorker(t, "test.echo", "wrong-output", 0)
		r, req := scenario(t, s, w, "wrong")
		out, err := r.Execute(bg, req)
		if err != nil || out.Disposition != execution.Reported {
			t.Fatalf("out=%+v err=%v", out, err)
		}
		if _, err = reference.Verify(bg, s, req, out, "verifier"); err == nil {
			t.Fatal("well-formed false claim verified")
		}
		if operation(t, s, req.OperationID).State != state.OperationVerifying {
			t.Fatal("false claim completed operation")
		}
	})
}

func TestStartFailureAfterAdmissionRemainsUnknown(t *testing.T) {
	withStores(t, func(t *testing.T, s *core.Store) {
		name := "invalid-worker"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte("synthetic invalid executable"), 0700); err != nil {
			t.Fatal(err)
		}
		w, err := New(Config{Executable: path}, "process-worker", "test.echo")
		if err != nil {
			t.Fatal(err)
		}
		r, req := scenario(t, s, w, "start-failure")
		out, err := r.Execute(bg, req)
		if err != nil || out.Disposition != execution.Unknown || out.Reason != execution.Unavailable || w.Snapshot().Started != 0 {
			t.Fatalf("start=%+v %v stats=%+v", out, err, w.Snapshot())
		}
		if operation(t, s, req.OperationID).State != state.OperationUnknownOutcome {
			t.Fatal("failed Start allocated reusable authority")
		}
		dup, err := r.Execute(bg, req)
		if err != nil || dup.Disposition != execution.Duplicate || w.Snapshot().Started != 0 {
			t.Fatal("failed Start retried automatically")
		}
	})
}

func TestProviderBudgetFailurePreservesStructuredReason(t *testing.T) {
	for _, limit := range []string{"output", "units"} {
		t.Run(limit, func(t *testing.T) {
			withStores(t, func(t *testing.T, s *core.Store) {
				w := newWorker(t, "test.digest", "", 0)
				r, req := scenario(t, s, w, "budget")
				if limit == "output" {
					req.Limits.OutputBytes = 1
				} else {
					req.Limits.Units = 1
				}
				out, err := r.Execute(bg, req)
				if err != nil || out.Disposition != execution.Unknown || out.Reason != execution.Budget {
					t.Fatalf("budget=%+v %v", out, err)
				}
				waitIdle(t, w)
				if w.Snapshot().Started != 1 || operation(t, s, req.OperationID).State != state.OperationUnknownOutcome {
					t.Fatal("provider limit changed certainty")
				}
			})
		})
	}
}

func TestLostOutputAfterReportIsNotReconstructed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s := openSQLite(t, path)
	defer func() { s.Close() }()
	w := newWorker(t, "test.digest", "", 0)
	r, req := scenario(t, s, w, "lost-output")
	out, err := r.Execute(bg, req)
	if err != nil || out.Disposition != execution.Reported {
		t.Fatalf("out=%+v %v", out, err)
	}
	waitIdle(t, w)
	out.Output = "" // only the report digest survives the controller loss
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openSQLite(t, path)
	if _, err := reference.Verify(bg, s, req, out, "verifier"); err == nil {
		t.Fatal("digest reconstructed a lost result")
	}
	op := operation(t, s, req.OperationID)
	if op.State != state.OperationVerifying {
		t.Fatal("unverified result became completion")
	}
	now := time.Now().UTC()
	obs := evidence.Record{ID: "lost-result", TaskID: op.Descriptor.TaskID, OperationID: req.OperationID, AttemptID: req.AttemptID, Kind: evidence.KindProcessObservation, SourceActorID: "controller", ObservedAt: now, SubjectRef: reference.Target, Summary: "result unavailable after controller restart"}
	if _, err := s.InvalidateAttempt(bg, req.OperationID, op.Revision, req.AttemptID, core.ControllerRestart, obs, now); err != nil {
		t.Fatal(err)
	}
	fresh := newWorker(t, "test.digest", "", 0)
	r = runnerFor(t, s, fresh, req)
	dup, err := r.Execute(bg, req)
	if err != nil || dup.Disposition != execution.Duplicate || fresh.Snapshot().Started != 0 || operation(t, s, req.OperationID).State != state.OperationUnknownOutcome {
		t.Fatal("lost output resumed automatically")
	}
}

func TestServeInInstrumentedChildAndCancelledInput(t *testing.T) {
	// With go test -race this child runs the actual Serve with instrumentation,
	// while the successful CLI tests above also cover the built entry point.
	s := core.NewStore()
	defer s.Close()
	w := newWorker(t, "test.echo", "serve", 0)
	r, req := scenario(t, s, w, "serve")
	out, err := r.Execute(bg, req)
	if err != nil || out.Disposition != execution.Reported {
		t.Fatalf("serve=%+v %v", out, err)
	}
	if _, err := reference.Verify(bg, s, req, out, "verifier"); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, w)
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inW.Close()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outR.Close()
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, inR, outW, "worker", "test.echo") }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled input accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve input wait ignored cancellation")
	}
	inR.Close()
	outW.Close()
}

func TestCancellationAndDeadlineKillOnlyOwnedChild(t *testing.T) {
	for _, kind := range []string{"cancel", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			withStores(t, func(t *testing.T, s *core.Store) {
				w := newWorker(t, "test.echo", "block", 0)
				r, req := scenario(t, s, w, "cancel")
				ctx, cancel := context.WithCancel(bg)
				defer cancel()
				if kind == "deadline" {
					var c context.CancelFunc
					ctx, c = context.WithTimeout(ctx, 2*time.Second)
					defer c()
				}
				done := make(chan execution.Outcome, 1)
				errs := make(chan error, 1)
				go func() { out, err := r.Execute(ctx, req); done <- out; errs <- err }()
				waitHeartbeats(t, w, 1)
				if kind == "cancel" {
					cancel()
				}
				select {
				case out := <-done:
					want := execution.Cancelled
					if kind == "deadline" {
						want = execution.TimedOut
					}
					if out.Disposition != execution.Unknown || out.Reason != want {
						t.Fatalf("out=%+v", out)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("unbounded controller wait")
				}
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
				waitIdle(t, w)
				if operation(t, s, req.OperationID).State != state.OperationUnknownOutcome {
					t.Fatal("cancellation proved completion")
				}
			})
		})
	}
}

func TestUnknownRestartRequiresExplicitVerifiedRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s := openSQLite(t, path)
	defer func() { s.Close() }()
	broken := newWorker(t, "test.digest", "crash", 0)
	r, req := scenario(t, s, broken, "restart")
	out, err := r.Execute(bg, req)
	if err != nil || out.Disposition != execution.Unknown {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	waitIdle(t, broken)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openSQLite(t, path)
	w := newWorker(t, "test.digest", "", 0)
	r = runnerFor(t, s, w, req)
	page, err := s.InspectRecovery(bg, "", 10)
	if err != nil || len(page.Items) != 1 || page.Items[0].Class != core.RecoveryUnknown || page.Items[0].Revocation == nil {
		t.Fatalf("recovery=%+v %v", page, err)
	}
	dup, err := r.Execute(bg, req)
	if err != nil || dup.Disposition != execution.Duplicate || w.Snapshot().Started != 0 {
		t.Fatal("restart dispatched unknown work")
	}
	op := operation(t, s, req.OperationID)
	now := time.Now().UTC()
	op, err = s.BeginVerification(bg, req.OperationID, op.Revision, now)
	if err != nil {
		t.Fatal(err)
	}
	// Trusted recovery establishes reaped owned process and the fixed, read-only
	// fixture. A general remote/mutating operation cannot use this policy.
	rec := evidence.Record{ID: "safe-retry", TaskID: op.Descriptor.TaskID, OperationID: req.OperationID, AttemptID: req.AttemptID, Kind: evidence.KindProcessObservation, SourceActorID: "verifier", ObservedAt: now, SubjectRef: reference.Target, Summary: "owned child reaped; synthetic read-only retry safe"}
	if err = s.RegisterEvidence(bg, rec); err != nil {
		t.Fatal(err)
	}
	op, err = s.ApplyVerification(bg, req.OperationID, op.Revision, evidence.Verification{ID: "retry-verdict", OperationID: req.OperationID, AttemptID: req.AttemptID, PolicyRef: reference.Policy, EvidenceIDs: []protocol.EvidenceID{rec.ID}, Verdict: evidence.VerdictNotSatisfiedRetryable, RetryExecutionSafe: true, ActualStateSummary: "reference retry safe after owned cleanup", VerifiedAt: now, VerifierActorID: "verifier"}, now)
	if err != nil {
		t.Fatal(err)
	}
	next := req
	next.MessageID = "offer-next"
	next.AttemptID = "attempt-next"
	next.LeaseID = "lease-next"
	next.RetryOf = req.AttemptID
	next.Revision = op.Revision
	next.IssuedAt = time.Now().UTC()
	next.Deadline = next.IssuedAt.Add(next.Limits.Duration)
	r = runnerFor(t, s, w, next)
	out, err = r.Execute(bg, next)
	if err != nil || out.Disposition != execution.Reported || w.Snapshot().Started != 1 {
		t.Fatalf("explicit retry=%+v %v", out, err)
	}
	op, err = reference.Verify(bg, s, next, out, "verifier")
	if err != nil || op.State != state.OperationSucceeded || op.LeaseGeneration != 2 {
		t.Fatalf("next=%+v %v", op, err)
	}
	if err = s.CheckActiveAttemptContext(bg, req.OperationID, req.AttemptID, req.LeaseID, 1, "process-worker"); !errors.Is(err, core.ErrStaleOwnership) {
		t.Fatal("old generation retained authority")
	}
}

func TestConcurrentOffersDoNotMultiplyProcesses(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			withStores(t, func(t *testing.T, s *core.Store) {
				w := newWorker(t, "test.echo", "", 0)
				r, req := scenario(t, s, w, "race")
				type result struct {
					out execution.Outcome
					err error
				}
				ch := make(chan result, 8)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := 0; i < 8; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						candidate := req
						if distinct {
							candidate.MessageID = protocol.MessageID(fmt.Sprintf("offer-%d", i))
							candidate.AttemptID = protocol.AttemptID(fmt.Sprintf("attempt-%d", i))
							candidate.LeaseID = protocol.LeaseID(fmt.Sprintf("lease-%d", i))
						}
						<-start
						out, err := r.Execute(bg, candidate)
						ch <- result{out, err}
					}(i)
				}
				close(start)
				wg.Wait()
				close(ch)
				reported, others := 0, 0
				for result := range ch {
					if result.err == nil && result.out.Disposition == execution.Reported {
						reported++
					} else if distinct && errors.Is(result.err, state.ErrRevisionConflict) {
						others++
					} else if !distinct && result.err == nil && result.out.Disposition == execution.Duplicate {
						others++
					} else {
						t.Fatalf("unexpected concurrent result: %+v %v", result.out, result.err)
					}
				}
				waitIdle(t, w)
				if reported != 1 || others != 7 || w.Snapshot().Started != 1 {
					t.Fatalf("reported=%d others=%d stats=%+v", reported, others, w.Snapshot())
				}
			})
		})
	}
}

func TestCapacityIsBoundedAndIndependentOperationsCanOverlap(t *testing.T) {
	s := core.NewStore()
	defer s.Close()
	w := newWorker(t, "test.echo", "block", 2)
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		r, req := scenario(t, s, w, fmt.Sprint(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := r.Execute(ctx, req)
			if err != nil {
				errs <- err
			} else if out.Disposition != execution.Unknown {
				errs <- fmt.Errorf("unexpected outcome %s", out.Disposition)
			}
		}()
	}
	waitHeartbeats(t, w, 2)
	if w.Snapshot().Active != 2 {
		t.Fatal("independent operations did not overlap")
	}
	r, req := scenario(t, s, w, "full")
	out, err := r.Execute(bg, req)
	if err != nil || out.Disposition != execution.Unknown || out.Reason != execution.Unavailable || w.Snapshot().Started != 2 {
		t.Fatalf("capacity=%+v %v %+v", out, err, w.Snapshot())
	}
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	waitIdle(t, w)
}

func TestAdmissionAndEnvironmentBoundaries(t *testing.T) {
	t.Setenv("ACP_R5_TEST_SECRET", "test-only-do-not-inherit")
	s := core.NewStore()
	defer s.Close()
	w := newWorker(t, "test.echo", "environment", 0)
	r, req := scenario(t, s, w, "env")
	out, err := r.Execute(bg, req)
	if err != nil || out.Disposition != execution.Reported {
		t.Fatalf("environment=%+v %v", out, err)
	}
	for _, mutate := range []func(*execution.Request){func(r *execution.Request) { r.DataClass = "private" }, func(r *execution.Request) { r.Model = "unknown" }, func(r *execution.Request) { r.Input = strings.Repeat("x", 1025) }, func(r *execution.Request) { r.Input = "\xff" }} {
		bad := req
		bad.MessageID = "bad"
		mutate(&bad)
		out, err = r.Execute(bg, bad)
		if (err == nil && out.Disposition != execution.Rejected) || w.Snapshot().Started != 1 {
			t.Fatalf("bad request dispatched: %+v %v", out, err)
		}
	}
	if _, err = New(Config{Executable: "relative-worker"}, "w", "test.echo"); err == nil {
		t.Fatal("relative executable accepted")
	}
	if _, err = New(Config{Executable: workerBinary}, "w", "unknown"); !errors.Is(err, execution.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err = New(Config{Executable: workerBinary, MaxParallel: 33}, "w", "test.echo"); !errors.Is(err, execution.ErrLimit) {
		t.Fatal(err)
	}
}
