package execution

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/dilukhin/agent_control_plane/internal/core"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/memory"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var errReviewCommit = errors.New("injected uncertainty after real commit")

type reviewUncertainCommit struct {
	p.Repository
	mu        sync.Mutex
	operation protocol.OperationID
	from, to  state.OperationState
	fired     bool
}

type reviewTransitionTx struct {
	p.Tx
	operation protocol.OperationID
	from, to  state.OperationState
	matched   bool
}

func (t *reviewTransitionTx) UpdateOperation(next state.Operation, revision uint64) error {
	before, err := t.Tx.Operation(next.Descriptor.ID)
	if err != nil {
		return err
	}
	if err := t.Tx.UpdateOperation(next, revision); err != nil {
		return err
	}
	if next.Descriptor.ID == t.operation && before.State == t.from && next.State == t.to {
		t.matched = true
	}
	return nil
}

func (r *reviewUncertainCommit) Update(ctx context.Context, fn func(p.Tx) error) error {
	r.mu.Lock()
	operation, from, to := r.operation, r.from, r.to
	r.mu.Unlock()
	var matched bool
	if err := r.Repository.Update(ctx, func(tx p.Tx) error {
		wrapped := &reviewTransitionTx{Tx: tx, operation: operation, from: from, to: to}
		if err := fn(wrapped); err != nil {
			return err
		}
		matched = wrapped.matched
		return nil
	}); err != nil {
		return err
	}
	// Inject only after the underlying transaction really committed the chosen
	// state transition. Additional progress/evidence writes do not affect this.
	r.mu.Lock()
	defer r.mu.Unlock()
	if matched && !r.fired {
		r.fired = true
		return errReviewCommit
	}
	return nil
}

func TestCoordinatorReviewCommitUncertainty(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, transition := range []struct {
			name     string
			from, to state.OperationState
		}{
			{"acceptance", state.OperationReady, state.OperationExecuting},
			{"result", state.OperationExecuting, state.OperationVerifying},
		} {
			t.Run(backend+"/"+transition.name, func(t *testing.T) {
				var repo p.Repository = memory.New()
				if backend == "sqlite" {
					var err error
					repo, err = sqlite.Open(bg, filepath.Join(t.TempDir(), "state.db"), sqlite.Options{})
					if err != nil {
						t.Fatal(err)
					}
				}
				wrapped := &reviewUncertainCommit{Repository: repo}
				s := core.NewWithRepository(wrapped)
				defer s.Close()
				provider := &fakeProvider{run: normal}
				r, req, _ := setup(t, s, provider)
				wrapped.mu.Lock()
				wrapped.operation, wrapped.from, wrapped.to = req.OperationID, transition.from, transition.to
				wrapped.mu.Unlock()
				out, err := r.Execute(bg, req)
				if !errors.Is(err, errReviewCommit) {
					t.Fatalf("out=%+v err=%v", out, err)
				}
				op := getOp(t, s)
				wantCalls := int32(0)
				wantState := state.OperationExecuting
				if transition.name == "result" {
					wantCalls = 1
					wantState = state.OperationUnknownOutcome
					if out.Disposition != Unknown {
						t.Fatalf("out=%+v", out)
					}
					attempt, err := s.GetAttempt(bg, req.AttemptID)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.CheckActiveAttemptContext(bg, req.OperationID, attempt.ID, attempt.LeaseID, attempt.LeaseGeneration, attempt.OwnerActorID); !errors.Is(err, core.ErrStaleOwnership) {
						t.Fatalf("uncertain attempt retains authority: %v", err)
					}
					page, err := s.InspectRecovery(bg, "", 10)
					if err != nil || len(page.Items) != 1 || page.Items[0].Revocation == nil || page.Items[0].Revocation.AttemptID != req.AttemptID {
						t.Fatalf("revocation not persisted: page=%+v err=%v", page, err)
					}
				}
				if provider.calls.Load() != wantCalls || op.State != wantState {
					t.Fatalf("calls=%d state=%s", provider.calls.Load(), op.State)
				}
				// Retrying delivery after reading state must never dispatch again.
				out, err = r.Execute(bg, req)
				if err != nil || out.Disposition != Duplicate || provider.calls.Load() != wantCalls {
					t.Fatalf("replay out=%+v err=%v calls=%d", out, err, provider.calls.Load())
				}
			})
		}
	}
}

func TestCoordinatorReviewCompetingDistinctOffers(t *testing.T) {
	stores(t, func(t *testing.T, s *core.Store) {
		provider := &fakeProvider{run: normal}
		r, req, _ := setup(t, s, provider)
		start := make(chan struct{})
		type result struct {
			index int
			out   Outcome
			err   error
		}
		results := make(chan result, 8)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				candidate := req
				candidate.MessageID = protocol.MessageID(fmt.Sprintf("offer-%d", i))
				candidate.AttemptID = protocol.AttemptID(fmt.Sprintf("attempt-%d", i))
				candidate.LeaseID = protocol.LeaseID(fmt.Sprintf("lease-%d", i))
				<-start
				out, err := r.Execute(bg, candidate)
				results <- result{i, out, err}
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		reported, conflicts, received := 0, 0, 0
		for got := range results {
			received++
			if got.err == nil {
				if got.out.Disposition != Reported || !got.out.Applied || got.out.Reason != OK {
					t.Fatalf("offer %d returned unexpected success: %+v", got.index, got.out)
				}
				reported++
			} else if errors.Is(got.err, state.ErrRevisionConflict) {
				// Distinct offers sharing the original revision lose the CAS race.
				conflicts++
			} else {
				t.Fatalf("offer %d failed outside the concurrency contract: out=%+v err=%v", got.index, got.out, got.err)
			}
		}
		if received != 8 || reported != 1 || conflicts != 7 {
			t.Fatalf("received=%d reported=%d conflicts=%d", received, reported, conflicts)
		}
		if provider.calls.Load() != 1 || getOp(t, s).State != state.OperationVerifying {
			t.Fatalf("calls=%d op=%+v", provider.calls.Load(), getOp(t, s))
		}
	})
}
