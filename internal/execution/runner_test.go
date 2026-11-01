package execution

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var bg = context.Background()

func stores(t *testing.T, f func(*testing.T, *core.Store)) {
	t.Helper()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			s := core.NewStore()
			if backend == "sqlite" {
				s = open(t, filepath.Join(t.TempDir(), "state.db"))
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			f(t, s)
		})
	}
}
func open(t *testing.T, path string) *core.Store {
	t.Helper()
	repo, err := sqlite.Open(bg, path, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return core.NewWithRepository(repo)
}
func setup(t *testing.T, s *core.Store, p Provider) (*Runner, Request, Grant) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := s.CreateTask(bg, "task", now); err != nil {
		t.Fatal(err)
	}
	d := protocol.OperationDescriptor{ID: "op", TaskID: "task", Name: "analyze", TargetRef: "fixture", EffectClass: protocol.EffectReadOnly, IdempotencyMode: protocol.IdempotencyIdempotent, VerificationPolicyRef: "check/v1", AuthorizationPolicyRef: "allow/v1"}
	op, err := s.CreateOperation(bg, d, now)
	if err != nil {
		t.Fatal(err)
	}
	op, err = s.MarkReady(bg, "op", op.Revision, now)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{Version: Version, MessageID: "offer", OperationID: "op", AttemptID: "attempt", LeaseID: "lease", CorrelationID: "chain", Revision: op.Revision, IssuedAt: now, Deadline: now.Add(time.Minute), Provider: p.ID(), Model: p.Models()[0], DataClass: "synthetic", Input: "fixture input", Limits: Limits{time.Minute, 1024, 1024, 1024}}
	g := Grant{PolicyRef: "allow/v1", Worker: "worker", Provider: p.ID(), Model: p.Models()[0], Action: "analyze", Target: "fixture", Effect: protocol.EffectReadOnly, DataClass: "synthetic", Expires: now.Add(2 * time.Minute), Limits: req.Limits}
	w, err := NewLocal("worker", p)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRunner(s, "controller", w, NewExactPolicy([]Grant{g}))
	if err != nil {
		t.Fatal(err)
	}
	return r, req, g
}
func getOp(t *testing.T, s *core.Store) state.Operation {
	t.Helper()
	v, e := s.GetOperation(bg, "op")
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func boundJob(t *testing.T, s *core.Store, req Request) Job {
	t.Helper()
	a, e := s.GetAttempt(bg, req.AttemptID)
	if e != nil {
		t.Fatal(e)
	}
	return Job{Request: req, Descriptor: getOp(t, s).Descriptor, Worker: a.OwnerActorID, Generation: a.LeaseGeneration}
}
func TestProvidersReplaceWithoutControllerChanges(t *testing.T) {
	for _, p := range []Provider{EchoProvider{}, DigestProvider{}} {
		t.Run(p.ID(), func(t *testing.T) {
			stores(t, func(t *testing.T, s *core.Store) {
				r, req, _ := setup(t, s, p)
				out, err := r.Execute(bg, req)
				if err != nil || out.Disposition != Reported || !out.Applied || out.Report.Digest != digest(out.Output) {
					t.Fatalf("%+v %v", out, err)
				}
				if getOp(t, s).State != state.OperationVerifying {
					t.Fatal("claim became completion")
				}
				records, err := s.EvidenceForOperation(bg, "op")
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range records {
					if v.Summary == req.Input || strings.Contains(v.Summary, out.Output) {
						t.Fatal("raw output persisted")
					}
				}
				j := boundJob(t, s, req)
				if applied, dup, err := r.report(bg, j, out.Report); err != nil || !dup || applied {
					t.Fatalf("report replay %v %v %v", applied, dup, err)
				}
				changed := out.Report
				changed.Digest = digest("different")
				if _, _, err := r.report(bg, j, changed); !errors.Is(err, core.ErrMessageIDCollision) {
					t.Fatalf("collision: %v", err)
				}
				before := getOp(t, s)
				if out, err := r.Execute(bg, req); err != nil || out.Disposition != Duplicate {
					t.Fatalf("offer replay %+v %v", out, err)
				}
				if !reflect.DeepEqual(getOp(t, s), before) {
					t.Fatal("duplicate changed state")
				}
				// A new message ID with the same attempt is not a new dispatch.
				req.MessageID = "other-offer"
				if _, err := r.Execute(bg, req); err == nil {
					t.Fatal("semantic replay accepted")
				}
			})
		})
	}
}

type fakeProvider struct {
	calls    atomic.Int32
	external bool
	run      func(context.Context, ProviderRequest) (Claim, error)
}

func (*fakeProvider) ID() string       { return "test.fake" }
func (*fakeProvider) Models() []string { return []string{"fake-v1"} }
func (p *fakeProvider) External() bool { return p.external }
func (p *fakeProvider) Generate(ctx context.Context, r ProviderRequest) (Claim, error) {
	p.calls.Add(1)
	return p.run(ctx, r)
}
func normal(ctx context.Context, r ProviderRequest) (Claim, error) {
	return Claim{Output: "done", Units: 1}, nil
}

func TestAuthorizationAndCapabilityRejections(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Request, *Grant, *fakeProvider)
		want   Reason
	}{
		{"model", func(r *Request, g *Grant, p *fakeProvider) { r.Model = "other" }, Unsupported},
		{"provider", func(r *Request, g *Grant, p *fakeProvider) { r.Provider = "other" }, Unsupported},
		{"action", func(r *Request, g *Grant, p *fakeProvider) { g.Action = "apply" }, Denied},
		{"target", func(r *Request, g *Grant, p *fakeProvider) { g.Target = "elsewhere" }, Denied},
		{"worker", func(r *Request, g *Grant, p *fakeProvider) { g.Worker = "other" }, Denied},
		{"policy version", func(r *Request, g *Grant, p *fakeProvider) { g.PolicyRef = "allow/v0" }, Denied},
		{"data class", func(r *Request, g *Grant, p *fakeProvider) { r.DataClass = "private" }, Denied},
		{"external data", func(r *Request, g *Grant, p *fakeProvider) { p.external = true }, Denied},
		{"expiry", func(r *Request, g *Grant, p *fakeProvider) { g.Expires = time.Now().Add(-time.Second) }, Denied},
		{"deadline past grant", func(r *Request, g *Grant, p *fakeProvider) { g.Expires = r.Deadline.Add(-time.Second) }, Denied},
		{"budget", func(r *Request, g *Grant, p *fakeProvider) { g.Limits.Units = 1 }, Denied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stores(t, func(t *testing.T, s *core.Store) {
				p := &fakeProvider{run: normal}
				r, req, g := setup(t, s, p)
				tc.change(&req, &g, p)
				r.policy = NewExactPolicy([]Grant{g})
				before := getOp(t, s)
				out, err := r.Execute(bg, req)
				if err != nil || out.Disposition != Rejected || out.Reason != tc.want || p.calls.Load() != 0 {
					t.Fatalf("%+v %v calls=%d", out, err, p.calls.Load())
				}
				if !reflect.DeepEqual(getOp(t, s), before) {
					t.Fatal("rejection allocated authority")
				}
				if _, err := s.GetAttempt(bg, req.AttemptID); !errors.Is(err, core.ErrNotFound) {
					t.Fatal(err)
				}
			})
		})
	}
}
func TestFailureAndCancellationRemainUnknown(t *testing.T) {
	cases := []struct {
		name string
		run  func(context.Context, ProviderRequest) (Claim, error)
		want Reason
	}{
		{"unavailable", func(context.Context, ProviderRequest) (Claim, error) { return Claim{}, ErrUnavailable }, Unavailable},
		{"error redaction", func(context.Context, ProviderRequest) (Claim, error) {
			return Claim{}, errors.New("password=must-never-persist")
		}, Failed},
		{"panic redaction", func(context.Context, ProviderRequest) (Claim, error) { panic("private provider detail") }, Failed},
		{"timeout", func(ctx context.Context, _ ProviderRequest) (Claim, error) { <-ctx.Done(); return Claim{}, ctx.Err() }, TimedOut},
		{"oversize output", func(context.Context, ProviderRequest) (Claim, error) {
			return Claim{Output: strings.Repeat("x", 1025)}, nil
		}, Budget},
		{"excess units", func(context.Context, ProviderRequest) (Claim, error) { return Claim{Units: 1025}, nil }, Budget},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stores(t, func(t *testing.T, s *core.Store) {
				p := &fakeProvider{run: tc.run}
				r, req, _ := setup(t, s, p)
				ctx := bg
				var cancel context.CancelFunc
				if tc.name == "timeout" {
					ctx, cancel = context.WithTimeout(bg, 100*time.Millisecond)
					defer cancel()
				}
				out, err := r.Execute(ctx, req)
				if err != nil || out.Disposition != Unknown || out.Reason != tc.want {
					t.Fatalf("%+v %v", out, err)
				}
				if getOp(t, s).State != state.OperationUnknownOutcome {
					t.Fatal("failure was treated as known")
				}
				a, err := s.GetAttempt(bg, req.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.CheckActiveAttemptContext(bg, "op", a.ID, a.LeaseID, a.LeaseGeneration, a.OwnerActorID); !errors.Is(err, core.ErrStaleOwnership) {
					t.Fatal(err)
				}
				out, err = r.Execute(bg, req)
				if err != nil || out.Disposition != Duplicate || p.calls.Load() != 1 {
					t.Fatalf("unsafe retry %+v %v", out, err)
				}
				records, _ := s.EvidenceForOperation(bg, "op")
				for _, v := range records {
					if strings.Contains(v.Summary, "password") || strings.Contains(v.Summary, "private provider") {
						t.Fatal("raw error persisted")
					}
				}
			})
		})
	}
}
func TestConcurrentDuplicateOffer(t *testing.T) {
	stores(t, func(t *testing.T, s *core.Store) {
		p := &fakeProvider{run: normal}
		r, req, _ := setup(t, s, p)
		var wg sync.WaitGroup
		results := make(chan Outcome, 8)
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); out, err := r.Execute(bg, req); results <- out; errs <- err }()
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		n := 0
		for out := range results {
			if out.Disposition == Reported {
				n++
			} else if out.Disposition != Duplicate {
				t.Fatalf("%+v", out)
			}
		}
		if n != 1 || p.calls.Load() != 1 {
			t.Fatalf("reports=%d calls=%d", n, p.calls.Load())
		}
	})
}
func TestCancelDoesNotProveExecutorStopped(t *testing.T) {
	stores(t, func(t *testing.T, s *core.Store) {
		entered := make(chan struct{})
		release := make(chan struct{})
		exited := make(chan struct{})
		p := &fakeProvider{run: func(context.Context, ProviderRequest) (Claim, error) {
			close(entered)
			<-release
			defer close(exited)
			return Claim{Output: "late"}, nil
		}}
		r, req, _ := setup(t, s, p)
		ctx, cancel := context.WithCancel(bg)
		done := make(chan Outcome, 1)
		errs := make(chan error, 1)
		go func() { v, e := r.Execute(ctx, req); done <- v; errs <- e }()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("not dispatched")
		}
		cancel()
		select {
		case out := <-done:
			if out.Disposition != Unknown || out.Reason != Cancelled {
				t.Fatalf("%+v", out)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("controller blocked")
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		// Cancellation returned while provider is still running. New dispatch denied.
		if getOp(t, s).State != state.OperationUnknownOutcome {
			t.Fatal("cancellation completed operation")
		}
		close(release)
		<-exited
		if getOp(t, s).State != state.OperationUnknownOutcome {
			t.Fatal("late return completed operation")
		}
	})
}
func TestRestartAndStaleClaimPreserveEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s := open(t, path)
	p := &fakeProvider{run: normal}
	r, req, _ := setup(t, s, p)
	out, err := r.Execute(bg, req)
	if err != nil {
		t.Fatal(err)
	}
	j := boundJob(t, s, req)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	r.store = s
	before := getOp(t, s)
	out2, err := r.Execute(bg, req)
	if err != nil || out2.Disposition != Duplicate || p.calls.Load() != 1 || !reflect.DeepEqual(getOp(t, s), before) {
		t.Fatalf("reopen replay %+v %v", out2, err)
	}
	if _, dup, err := r.report(bg, j, out.Report); err != nil || !dup {
		t.Fatalf("reopen report %v %v", dup, err)
	}
	page, err := s.InspectRecovery(bg, "", 10)
	if err != nil || len(page.Items) != 1 || page.Items[0].Class != core.RecoveryEvaluate {
		t.Fatalf("%+v %v", page, err)
	}
	// Trusted recovery decision invalidates only the known attempt.
	now := time.Now().UTC()
	obs := record(j, "restart", evidence.KindProcessObservation, "controller", now, "restart")
	op, err := s.InvalidateAttempt(bg, "op", before.Revision, req.AttemptID, core.ControllerRestart, obs, now)
	if err != nil {
		t.Fatal(err)
	}
	// Separate late report exercises retention without colliding with first result.
	rec := record(j, "late-claim", evidence.KindWorkerReport, "worker", now, "late")
	env := event(j, "late-msg", "operation.result_reported", "worker", now, nil)
	var applied bool
	_, err = s.ProcessMessage(bg, env, func(tx *core.Transaction) error {
		var e error
		applied, e = tx.RecordWorkerReport(rec, req.LeaseID, j.Generation, req.Deadline)
		return e
	})
	if err != nil || applied || !reflect.DeepEqual(getOp(t, s), op) {
		t.Fatalf("late claim applied=%v %v", applied, err)
	}
	records, _ := s.EvidenceForOperation(bg, "op")
	found := false
	for _, v := range records {
		found = found || v.ID == "late-claim"
	}
	if !found {
		t.Fatal("late evidence lost")
	}
}

type testWorker struct {
	base   Worker
	accept error
	run    func(context.Context, Job, func(context.Context) error) (Claim, error)
}

func (w testWorker) Capabilities() Capabilities { return w.base.Capabilities() }
func (w testWorker) Accept(j Job) error {
	if w.accept != nil {
		return w.accept
	}
	return w.base.Accept(j)
}
func (w testWorker) Run(ctx context.Context, j Job, h func(context.Context) error) (Claim, error) {
	return w.run(ctx, j, h)
}
func TestWorkerRefusalAndHeartbeatBudget(t *testing.T) {
	for _, refusal := range []error{ErrUnavailable, ErrUnsupported} {
		stores(t, func(t *testing.T, s *core.Store) {
			p := &fakeProvider{run: normal}
			r, req, _ := setup(t, s, p)
			r.worker = testWorker{base: r.worker, accept: refusal}
			out, err := r.Execute(bg, req)
			if err != nil || out.Disposition != Rejected || out.Reason != reason(refusal) || p.calls.Load() != 0 {
				t.Fatalf("%+v %v", out, err)
			}
		})
	}
	stores(t, func(t *testing.T, s *core.Store) {
		r, req, _ := setup(t, s, EchoProvider{})
		r.worker = testWorker{base: r.worker, run: func(ctx context.Context, j Job, h func(context.Context) error) (Claim, error) {
			for i := 0; i < MaxHeartbeats+1; i++ {
				if err := h(ctx); err != nil {
					return Claim{}, err
				}
			}
			t.Error("heartbeat budget did not stop worker")
			return Claim{}, nil
		}}
		out, err := r.Execute(bg, req)
		if err != nil || out.Disposition != Unknown || out.Reason != Budget {
			t.Fatalf("%+v %v", out, err)
		}
		records, _ := s.EvidenceForOperation(bg, "op")
		if len(records) != MaxHeartbeats+2 {
			t.Fatalf("unbounded history: %d", len(records))
		}
	})
}

type revocablePolicy struct {
	base    Policy
	revoked atomic.Bool
}

func (p *revocablePolicy) Check(j Job, c Capabilities, now time.Time) error {
	if p.revoked.Load() {
		return ErrDenied
	}
	return p.base.Check(j, c, now)
}
func TestRevocationAtCheckpointAndBeforeResult(t *testing.T) {
	for _, checkpoint := range []bool{true, false} {
		stores(t, func(t *testing.T, s *core.Store) {
			r, req, _ := setup(t, s, EchoProvider{})
			p := &revocablePolicy{base: r.policy}
			r.policy = p
			r.worker = testWorker{base: r.worker, run: func(ctx context.Context, j Job, h func(context.Context) error) (Claim, error) {
				p.revoked.Store(true)
				if checkpoint {
					return Claim{}, h(ctx)
				}
				return Claim{Output: "untrusted result"}, nil
			}}
			out, err := r.Execute(bg, req)
			if err != nil || out.Disposition != Unknown || out.Reason != Denied {
				t.Fatalf("%+v %v", out, err)
			}
		})
	}
}
func TestLeaseExpiryAndInvalidRequests(t *testing.T) {
	stores(t, func(t *testing.T, s *core.Store) {
		p := &fakeProvider{run: func(ctx context.Context, _ ProviderRequest) (Claim, error) { <-ctx.Done(); return Claim{}, ctx.Err() }}
		r, req, _ := setup(t, s, p)
		for _, change := range []func(*Request){
			func(r *Request) { r.Version = "2.0" }, func(r *Request) { r.Deadline = time.Now().Add(-time.Second) },
			func(r *Request) { r.Limits.Duration = 0 }, func(r *Request) { r.Input = strings.Repeat("x", 1025) },
			func(r *Request) { r.Deadline = r.IssuedAt.Add(MaxDuration + time.Second) },
		} {
			bad := req
			change(&bad)
			if _, err := r.Execute(bg, bad); err == nil {
				t.Fatal("invalid request accepted")
			}
		}
		if p.calls.Load() != 0 {
			t.Fatal("invalid request dispatched")
		}
		req.Deadline = time.Now().Add(100 * time.Millisecond)
		out, err := r.Execute(bg, req)
		if err != nil || out.Reason != TimedOut || out.Disposition != Unknown {
			t.Fatalf("%+v %v", out, err)
		}
	})
}
func TestExplicitVerifiedRetryAndOldGeneration(t *testing.T) {
	stores(t, func(t *testing.T, s *core.Store) {
		p := &fakeProvider{run: normal}
		r, req, _ := setup(t, s, p)
		out, err := r.Execute(bg, req)
		if err != nil {
			t.Fatal(err)
		}
		old := boundJob(t, s, req)
		op := getOp(t, s)
		now := time.Now().UTC()
		// This synthetic verifier is trusted and read-only. A real mutation verifier
		// must additionally establish quiescence or target fencing/idempotency.
		rec := record(old, "actual", evidence.KindTargetObservation, "verifier", now, "fixture requires retry")
		if err = s.RegisterEvidence(bg, rec); err != nil {
			t.Fatal(err)
		}
		v := evidence.Verification{ID: "verified-retry", OperationID: "op", AttemptID: req.AttemptID, PolicyRef: "check/v1", EvidenceIDs: []protocol.EvidenceID{rec.ID}, Verdict: evidence.VerdictNotSatisfiedRetryable, RetryExecutionSafe: true, ActualStateSummary: "synthetic read-only retry", VerifiedAt: now, VerifierActorID: "verifier"}
		op, err = s.ApplyVerification(bg, "op", op.Revision, v, now)
		if err != nil {
			t.Fatal(err)
		}
		next := req
		next.MessageID = "offer2"
		next.AttemptID = "attempt2"
		next.LeaseID = "lease2"
		next.RetryOf = req.AttemptID
		next.Revision = op.Revision
		missing := next
		missing.MessageID = "missing-retry-link"
		missing.RetryOf = ""
		denied, e := r.Execute(bg, missing)
		if e != nil || denied.Disposition != Rejected || denied.Reason != Denied || p.calls.Load() != 1 {
			t.Fatalf("missing retry link: %+v %v", denied, e)
		}
		out2, err := r.Execute(bg, next)
		if err != nil || out2.Disposition != Reported || p.calls.Load() != 2 {
			t.Fatalf("%+v %v", out2, err)
		}
		op = getOp(t, s)
		if op.LeaseGeneration != 2 {
			t.Fatal("generation not advanced")
		}
		if _, dup, err := r.report(bg, old, out.Report); err != nil || !dup {
			t.Fatalf("old duplicate %v %v", dup, err)
		}
		if !reflect.DeepEqual(op, getOp(t, s)) {
			t.Fatal("old generation changed successor")
		}
		if err = s.CheckActiveAttemptContext(bg, "op", req.AttemptID, req.LeaseID, old.Generation, "worker"); !errors.Is(err, core.ErrStaleOwnership) {
			t.Fatal(err)
		}
	})
}
func TestCoreRejectsForgedAuthorityAndExpiredResult(t *testing.T) {
	stores(t, func(t *testing.T, s *core.Store) {
		r, req, _ := setup(t, s, EchoProvider{})
		_ = r
		op, a, err := s.StartAttempt(bg, "op", req.Revision, req.AttemptID, req.LeaseID, "worker", "", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		j := boundJob(t, s, req)
		for i, change := range []func(*evidence.Record, *protocol.LeaseID, *uint64){
			func(v *evidence.Record, l *protocol.LeaseID, g *uint64) { v.Kind = evidence.KindTargetObservation },
			func(v *evidence.Record, l *protocol.LeaseID, g *uint64) { v.SourceActorID = "forged" },
			func(v *evidence.Record, l *protocol.LeaseID, g *uint64) { v.SubjectRef = "other" },
			func(v *evidence.Record, l *protocol.LeaseID, g *uint64) { *l = "other" },
			func(v *evidence.Record, l *protocol.LeaseID, g *uint64) { *g = 2 },
		} {
			now := time.Now().UTC()
			id := protocol.MessageID(string(rune('a' + i)))
			env := event(j, id, "operation.result_reported", "worker", now, nil)
			rec := record(j, string(id), evidence.KindWorkerReport, "worker", now, "claim")
			lease := a.LeaseID
			generation := a.LeaseGeneration
			change(&rec, &lease, &generation)
			_, err := s.ProcessMessage(bg, env, func(tx *core.Transaction) error {
				_, e := tx.RecordWorkerReport(rec, lease, generation, req.Deadline)
				return e
			})
			if !errors.Is(err, core.ErrEvidenceScope) {
				t.Fatalf("forgery accepted: %v", err)
			}
		}
		now := time.Now().UTC()
		rec := record(j, "expired-result", evidence.KindWorkerReport, "worker", now, "claim")
		_, err = s.ProcessMessage(bg, event(j, "expired-msg", "operation.result_reported", "worker", now, nil), func(tx *core.Transaction) error {
			_, e := tx.RecordWorkerReport(rec, a.LeaseID, a.LeaseGeneration, now.Add(-time.Second))
			return e
		})
		if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(op, getOp(t, s)) {
			t.Fatalf("expired result accepted: %v", err)
		}
		records, _ := s.EvidenceForOperation(bg, "op")
		if len(records) != 0 {
			t.Fatal("invalid evidence survived transaction rollback")
		}
	})
}
func TestRestartBeforeDispatchNeverReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s := open(t, path)
	p := &fakeProvider{run: normal}
	r, req, _ := setup(t, s, p)
	// Persisted attempt followed by process loss before dispatch is uncertain,
	// not proof that the command was never sent.
	_, _, err := s.StartAttempt(bg, "op", req.Revision, req.AttemptID, req.LeaseID, "worker", "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	r.store = s
	if _, err = r.Execute(bg, req); err == nil || p.calls.Load() != 0 {
		t.Fatal("restart dispatched active attempt")
	}
	page, err := s.InspectRecovery(bg, "", 10)
	if err != nil || page.Items[0].Class != core.RecoveryEvaluate {
		t.Fatalf("%+v %v", page, err)
	}
}

func TestLocalCannotApplyEvenWithGrant(t *testing.T) {
	w, err := NewLocal("worker", EchoProvider{})
	if err != nil {
		t.Fatal(err)
	}
	j := Job{Worker: "worker", Request: Request{Provider: "test.echo", Model: "echo-v1", DataClass: "synthetic", Limits: Limits{time.Minute, 128, 128, 128}}, Descriptor: protocol.OperationDescriptor{Name: "analyze", EffectClass: protocol.EffectMutation}}
	if err = w.Accept(j); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("local mutation accepted: %v", err)
	}
}
