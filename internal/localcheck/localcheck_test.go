package localcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

const outputSentinel = "private-output-not-for-state"

func TestLocalCommandHelper(t *testing.T) {
	mode := ""
	for i, arg := range os.Args {
		if arg == "--localcheck-helper" && i+1 < len(os.Args) {
			mode = os.Args[i+1]
		}
	}
	if mode == "" {
		return
	}
	if err := os.WriteFile("called.txt", []byte("one"), 0600); err != nil {
		os.Exit(9)
	}
	switch mode {
	case "fail":
		fmt.Fprint(os.Stderr, outputSentinel)
		os.Exit(7)
	case "slow":
		time.Sleep(10 * time.Second)
	case "over":
		fmt.Print(strings.Repeat("z", 100000))
	case "mutate":
		os.WriteFile("source.txt", []byte("changed"), 0600)
	case "env":
		fmt.Print(os.Getenv("ACP_TEST_PRIVATE_VALUE"))
	case "home":
		home, err := os.UserHomeDir()
		if err != nil {
			os.Exit(10)
		}
		fmt.Print(home)
	case "gate":
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat("release.txt"); err == nil {
				os.Exit(0)
			}
			time.Sleep(10 * time.Millisecond)
		}
		os.Exit(8)
	default:
		fmt.Print(outputSentinel)
	}
	os.Exit(0)
}

func fixture(t *testing.T, mode string) (Config, string, string) {
	t.Helper()
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "source.txt"), []byte("selected source"), 0600); err != nil {
		t.Fatal(err)
	}
	spec := Spec{Version: 1, Label: "fixture", Arguments: []string{"-test.run=^TestLocalCommandHelper$", "--", "--localcheck-helper", mode}, TimeoutSeconds: 5, MaxOutputBytes: 4096, Inputs: map[string]string{"source.txt": sum([]byte("selected source"))}}
	if mode == "slow" {
		spec.TimeoutSeconds = 1
	}
	b, _ := json.Marshal(spec)
	path := filepath.Join(t.TempDir(), "check.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c, err := Load(path, exe, workspace)
	if err != nil {
		t.Fatal(err)
	}
	return c, filepath.Join(t.TempDir(), "state"), path
}

func TestCheckSuccessReopenAndDuplicate(t *testing.T) {
	c, root, _ := fixture(t, "ok")
	r, output, err := Check(context.Background(), root, "success", c)
	if err != nil || r.TaskState != state.TaskCompleted || r.Operation != state.OperationSucceeded || r.ExitCode == nil || *r.ExitCode != 0 || string(output) != outputSentinel {
		t.Fatalf("%+v %v", r, err)
	}
	r2, err := Status(context.Background(), root, "success")
	if err != nil || r2.Digest != sum(output) || r2.TaskState != state.TaskCompleted {
		t.Fatalf("reopen %+v %v", r2, err)
	}
	_, _, err = Check(context.Background(), root, "success", c)
	if !errors.Is(err, ErrExisting) {
		t.Fatalf("duplicate: %v", err)
	}
	err = filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.IsDir() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(b, []byte(outputSentinel)) || bytes.Contains(b, []byte(c.Workspace)) {
				t.Fatal("raw output or workspace leaked into state")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestKnownFailureAndInputMutation(t *testing.T) {
	for _, mode := range []string{"fail", "mutate"} {
		t.Run(mode, func(t *testing.T) {
			c, root, _ := fixture(t, mode)
			r, _, err := Check(context.Background(), root, mode, c)
			if err != nil || r.Operation != state.OperationFailed || r.TaskState != state.TaskFailed {
				t.Fatalf("%+v %v", r, err)
			}
			if mode == "fail" && (r.ExitCode == nil || *r.ExitCode != 7) {
				t.Fatal(r)
			}
			if mode == "mutate" && (r.InputsMatch == nil || *r.InputsMatch) {
				t.Fatal(r)
			}
		})
	}
}

func TestUnknownOutcomesNeverRerun(t *testing.T) {
	for _, mode := range []string{"slow", "over"} {
		t.Run(mode, func(t *testing.T) {
			c, root, _ := fixture(t, mode)
			began := time.Now()
			r, _, err := Check(context.Background(), root, mode, c)
			if err == nil || r.Operation != state.OperationUnknownOutcome || time.Since(began) > 7*time.Second {
				t.Fatalf("%+v %v", r, err)
			}
			r, err = Status(context.Background(), root, mode)
			if err != nil || r.Operation != state.OperationUnknownOutcome {
				t.Fatalf("%+v %v", r, err)
			}
			if _, err := Recover(context.Background(), root, mode); !errors.Is(err, ErrReceipt) {
				t.Fatal(err)
			}
			if _, _, err := Check(context.Background(), root, mode, c); !errors.Is(err, ErrExisting) {
				t.Fatal(err)
			}
		})
	}
}

func TestEnvironmentNotInherited(t *testing.T) {
	t.Setenv("ACP_TEST_PRIVATE_VALUE", "do-not-inherit")
	c, root, _ := fixture(t, "env")
	r, output, err := Check(context.Background(), root, "environment", c)
	if err != nil || len(output) != 0 || r.Operation != state.OperationSucceeded {
		t.Fatalf("%+v %v bytes=%d", r, err, len(output))
	}
}

func TestWindowsHomeAvailableToChild(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows home-directory contract")
	}
	home := t.TempDir()
	t.Setenv("USERPROFILE", home)
	c, root, _ := fixture(t, "home")
	r, output, err := Check(context.Background(), root, "windows-home", c)
	if err != nil || string(output) != home || r.Operation != state.OperationSucceeded {
		t.Fatalf("child could not resolve configured home: operation=%s err=%v", r.Operation, err)
	}
}

func TestCommandEnvironmentExcludesCredentials(t *testing.T) {
	privateKeys := []string{"ACP_TEST_PRIVATE_VALUE", "OPENAI_API_KEY", "SSH_AUTH_SOCK", "AWS_SECRET_ACCESS_KEY", "HTTP_PROXY", "HOME"}
	for _, key := range privateKeys {
		t.Setenv(key, "must-not-reach-child")
	}
	for _, entry := range commandEnv() {
		key, _, _ := strings.Cut(entry, "=")
		for _, private := range privateKeys {
			if strings.EqualFold(key, private) {
				t.Fatalf("private environment key inherited: %s", key)
			}
		}
	}
}

func TestSpecBoundaries(t *testing.T) {
	c, root, path := fixture(t, "ok")
	for _, name := range []string{"../source.txt", "/source.txt", "a/../source.txt", "a\\source.txt", "C:source.txt", "a//source.txt"} {
		s := c.Spec
		s.Inputs = map[string]string{name: sum([]byte("selected source"))}
		b, _ := json.Marshal(s)
		os.WriteFile(path, b, 0600)
		if _, err := Load(path, c.Program, c.Workspace); err == nil {
			t.Fatal(name)
		}
	}
	b, _ := json.Marshal(c.Spec)
	for _, raw := range [][]byte{append(append([]byte{}, b...), []byte(" {}")...), []byte(`{"version":1,"unexpected":true}`), bytes.Repeat([]byte("x"), 65537)} {
		os.WriteFile(path, raw, 0600)
		if _, err := Load(path, c.Program, c.Workspace); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	os.WriteFile(filepath.Join(c.Workspace, "source.txt"), []byte("different"), 0600)
	if _, _, err := Check(context.Background(), root, "changed", c); !errors.Is(err, ErrInputs) {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("state created before input gate")
	}
}

func TestReceiptTamperingAndMissing(t *testing.T) {
	for _, change := range []string{"change", "delete"} {
		t.Run(change, func(t *testing.T) {
			c, root, _ := fixture(t, "ok")
			if _, _, err := Check(context.Background(), root, "receipt", c); err != nil {
				t.Fatal(err)
			}
			if change == "delete" {
				os.Remove(receiptPath(root, "receipt"))
			} else {
				r, err := loadReceipt(root, "receipt")
				if err != nil {
					t.Fatal(err)
				}
				r.ExitCode = 13
				b, _ := json.Marshal(r)
				os.WriteFile(receiptPath(root, "receipt"), b, 0600)
			}
			if _, err := Status(context.Background(), root, "receipt"); !errors.Is(err, ErrReceipt) {
				t.Fatal(err)
			}
			if _, err := Recover(context.Background(), root, "receipt"); !errors.Is(err, ErrReceipt) {
				t.Fatal(err)
			}
		})
	}
}

// Build a reported-but-unverified operation using public R2/R4 APIs, simulating
// a controller stop after durable receipt creation and before verification.
func reported(t *testing.T, c Config, root, id string) {
	t.Helper()
	ctx := context.Background()
	s, abs, err := open(ctx, root, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC()
	task := protocol.TaskID(id)
	if _, err = s.CreateTask(ctx, task, now); err != nil {
		t.Fatal(err)
	}
	d := protocol.OperationDescriptor{ID: operationID(id), TaskID: task, Name: "project.check", TargetRef: target(id), EffectClass: protocol.EffectMutation, ConflictScope: []string{"test-scope"}, IdempotencyMode: protocol.IdempotencyUnknown, VerificationPolicyRef: verifyPolicy, AuthorizationPolicyRef: "operator-local-check/v1"}
	op, err := s.CreateOperation(ctx, d, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterEvidence(ctx, evidence.Record{ID: protocol.EvidenceID("intent_" + id), TaskID: task, OperationID: d.ID, Kind: evidence.KindPolicyDecision, SourceActorID: controllerActor, ObservedAt: now, SubjectRef: target(id), Summary: "test intent", Digest: "sha256:" + c.Digest}); err != nil {
		t.Fatal(err)
	}
	op, err = s.MarkReady(ctx, d.ID, op.Revision, now)
	if err != nil {
		t.Fatal(err)
	}
	w := &commandWorker{config: c}
	limits := w.Capabilities().Limits
	end := now.Add(c.duration())
	policy := execution.NewExactPolicy([]execution.Grant{{PolicyRef: d.AuthorizationPolicyRef, Worker: workerActor, Provider: "local.command", Model: "process-v1", Action: d.Name, Target: d.TargetRef, DataClass: "local-project", Effect: d.EffectClass, Expires: end, Limits: limits}})
	runner, err := execution.NewRunner(s, controllerActor, w, policy)
	if err != nil {
		t.Fatal(err)
	}
	out, err := runner.Execute(ctx, execution.Request{Version: execution.Version, MessageID: protocol.MessageID("offer_" + id), OperationID: d.ID, AttemptID: protocol.AttemptID("att_" + id), LeaseID: protocol.LeaseID("lease_" + id), CorrelationID: protocol.CorrelationID("corr_" + id), Revision: op.Revision, IssuedAt: now, Deadline: end, Provider: "local.command", Model: "process-v1", DataClass: "local-project", Input: c.Digest, Limits: limits})
	if err != nil || out.Disposition != execution.Reported {
		t.Fatalf("%+v %v", out, err)
	}
	if err = saveReceipt(abs, id, *w.receipt); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverReportedAndRejectWrongBinding(t *testing.T) {
	for _, change := range []string{"none", "config", "generation"} {
		t.Run(change, func(t *testing.T) {
			c, root, _ := fixture(t, "ok")
			reported(t, c, root, "reported")
			if change != "none" {
				r, _ := loadReceipt(root, "reported")
				if change == "config" {
					r.Config = strings.Repeat("a", 64)
				} else {
					r.Generation++
				}
				b, _ := json.Marshal(r)
				os.WriteFile(receiptPath(root, "reported"), b, 0600)
			}
			r, err := Recover(context.Background(), root, "reported")
			if change == "none" {
				if err != nil || r.TaskState != state.TaskCompleted {
					t.Fatalf("%+v %v", r, err)
				}
				if _, err = Recover(context.Background(), root, "reported"); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrReceipt) {
				t.Fatal(err)
			}
		})
	}
}

func TestConcurrentSameTaskAndWorkspaceConflict(t *testing.T) {
	c, root, _ := fixture(t, "ok")
	s, _, err := open(context.Background(), root, true)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := Check(context.Background(), root, "concurrent", c)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		if err == nil {
			won++
		} else if !errors.Is(err, ErrExisting) {
			t.Fatal(err)
		}
	}
	if won != 1 {
		t.Fatal(won)
	}
	c, root, _ = fixture(t, "gate")
	done := make(chan error, 1)
	go func() { _, _, err := Check(context.Background(), root, "first", c); done <- err }()
	deadline := time.Now().Add(6 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(c.Workspace, "called.txt")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no child start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, err = Check(context.Background(), root, "second", c)
	if !errors.Is(err, core.ErrConflictScope) {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(c.Workspace, "release.txt"), []byte("go"), 0600)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(9 * time.Second):
		t.Fatal("not finished")
	}
}

func TestPartialTaskAndForeignStateArePreserved(t *testing.T) {
	c, root, _ := fixture(t, "ok")
	s, _, err := open(context.Background(), root, true)
	if err != nil {
		t.Fatal(err)
	}
	s.CreateTask(context.Background(), "partial", time.Now().UTC())
	s.Close()
	if _, _, err := Check(context.Background(), root, "partial", c); !errors.Is(err, ErrExisting) {
		t.Fatal(err)
	}
	foreign := t.TempDir()
	os.WriteFile(filepath.Join(foreign, "foreign.txt"), []byte("keep"), 0600)
	if _, _, err := Check(context.Background(), foreign, "foreign", c); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(foreign, "foreign.txt")); string(b) != "keep" {
		t.Fatal("foreign state changed")
	}
}

func TestBoundedCollectionKeepsLiveAndRemovesOldOwnedReceipt(t *testing.T) {
	c, root, _ := fixture(t, "ok")
	if _, _, err := Check(context.Background(), root, "live", c); err != nil {
		t.Fatal(err)
	}
	old, _ := loadReceipt(root, "live")
	old.Task = "old"
	old.Operation = operationID("old")
	old.Attempt = "att_old"
	old.Lease = "lease_old"
	old.Finished = time.Now().Add(-31 * 24 * time.Hour)
	if err := saveReceipt(root, "old", old); err != nil {
		t.Fatal(err)
	}
	r, err := Collect(context.Background(), root, "", "live")
	if err != nil || r.RemovedReceipts != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	r, err = Collect(context.Background(), root, "", "old")
	if err != nil || r.RemovedReceipts != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err = os.Stat(receiptPath(root, "old")); !os.IsNotExist(err) {
		t.Fatal("old receipt retained")
	}
	if _, err := Status(context.Background(), root, "live"); err != nil {
		t.Fatal(err)
	}
}

func TestCancelledContextAndInvalidTaskDoNotExecute(t *testing.T) {
	c, root, _ := fixture(t, "ok")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := Check(ctx, root, "cancelled", c); err == nil {
		t.Fatal("cancel ignored")
	}
	if _, _, err := Check(context.Background(), root, "../escape", c); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.Workspace, "called.txt")); !os.IsNotExist(err) {
		t.Fatal("unexpected command")
	}
}

func TestPrepareOnlyEquivalentLineEndings(t *testing.T) {
	c, _, specPath := fixture(t, "ok")
	c.Spec.Inputs["source.txt"] = sum([]byte("first\nsecond\n"))
	b, _ := json.Marshal(c.Spec)
	os.WriteFile(specPath, b, 0600)
	input := filepath.Join(c.Workspace, "source.txt")
	os.WriteFile(input, []byte("first\r\nsecond\r\n"), 0600)
	output := filepath.Join(t.TempDir(), "local.json")
	if err := Prepare(specPath, c.Program, c.Workspace, output); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(output, c.Program, c.Workspace); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(output)
	if err := Prepare(specPath, c.Program, c.Workspace, output); err == nil {
		t.Fatal("replaced existing profile")
	}
	if after, _ := os.ReadFile(output); !bytes.Equal(before, after) {
		t.Fatal("output changed")
	}
	os.WriteFile(input, []byte("other code\r\n"), 0600)
	other := filepath.Join(t.TempDir(), "other.json")
	if err := Prepare(specPath, c.Program, c.Workspace, other); !errors.Is(err, ErrInputs) {
		t.Fatal(err)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatal("created profile for changed source")
	}
	if _, err := os.Stat(filepath.Join(c.Workspace, "called.txt")); !os.IsNotExist(err) {
		t.Fatal("prepare executed program")
	}
}
