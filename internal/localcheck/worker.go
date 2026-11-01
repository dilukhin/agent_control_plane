package localcheck

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

const workerActor protocol.ActorID = "local-check-executor"
const controllerActor protocol.ActorID = "local-check-controller"

type Receipt struct {
	Version     int                  `json:"version"`
	Task        protocol.TaskID      `json:"task"`
	Operation   protocol.OperationID `json:"operation"`
	Attempt     protocol.AttemptID   `json:"attempt"`
	Lease       protocol.LeaseID     `json:"lease"`
	Generation  uint64               `json:"generation"`
	Config      string               `json:"config_digest"`
	ExitCode    int                  `json:"exit_code"`
	OutputBytes int                  `json:"output_bytes"`
	OutputHash  string               `json:"output_sha256"`
	InputsMatch bool                 `json:"inputs_match"`
	Finished    time.Time            `json:"finished"`
}

type boundedOutput struct {
	mu     sync.Mutex
	data   []byte
	limit  int
	over   bool
	cancel context.CancelFunc
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.over {
		if len(p) > b.limit-len(b.data) {
			b.over = true
			b.cancel()
		} else {
			b.data = append(b.data, p...)
		}
	}
	return len(p), nil
}

type commandWorker struct {
	config  Config
	mu      sync.Mutex
	receipt *Receipt
	output  []byte
}

func (w *commandWorker) Capabilities() execution.Capabilities {
	return execution.Capabilities{Version: execution.Version, Worker: workerActor, Provider: "local.command", Models: []string{"process-v1"}, Actions: []string{"project.check"}, Effects: []protocol.EffectClass{protocol.EffectMutation}, DataClasses: []string{"local-project"}, Limits: execution.Limits{Duration: w.config.duration(), InputBytes: 64, OutputBytes: 4096, Units: 1}}
}

func (w *commandWorker) Accept(j execution.Job) error {
	if j.Worker != workerActor || j.Descriptor.Name != "project.check" || j.Descriptor.EffectClass != protocol.EffectMutation || j.Request.Provider != "local.command" || j.Request.Model != "process-v1" || j.Request.Input != w.config.Digest || j.Request.DataClass != "local-project" {
		return execution.ErrDenied
	}
	return nil
}

func commandEnv() []string {
	env := make([]string, 0, 10)
	for _, k := range []string{"SYSTEMROOT", "WINDIR", "TEMP", "TMP", "PATH"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return append(env, "PYTHONDONTWRITEBYTECODE=1", "PYTHONUTF8=1", "PYTHONIOENCODING=utf-8")
}

// Run adapts a trusted operator-selected executable to R4. No command, shell,
// path or environment comes from the request. It observes only the direct child.
func (w *commandWorker) Run(ctx context.Context, j execution.Job, heartbeat func(context.Context) error) (execution.Claim, error) {
	if err := w.Accept(j); err != nil {
		return execution.Claim{}, err
	}
	if err := w.config.CheckInputs(); err != nil {
		return execution.Claim{}, err
	}
	if _, err := regular(w.config.Program); err != nil {
		return execution.Claim{}, execution.ErrUnavailable
	}
	b, err := os.ReadFile(w.config.Program)
	if err != nil || sum(b) != w.config.programSum {
		return execution.Claim{}, execution.ErrUnavailable
	}
	if err := heartbeat(ctx); err != nil {
		return execution.Claim{}, err
	}
	ctx, cancel := context.WithDeadline(ctx, j.Request.Deadline)
	defer cancel()
	sink := &boundedOutput{limit: w.config.Spec.MaxOutputBytes, cancel: cancel}
	cmd := exec.CommandContext(ctx, w.config.Program, w.config.Spec.Arguments...)
	cmd.Dir = w.config.Workspace
	cmd.Env = commandEnv()
	cmd.Stdout, cmd.Stderr = sink, sink
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Start(); err != nil {
		return execution.Claim{}, execution.ErrUnavailable
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		cmd.Process.Kill()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
		}
		return execution.Claim{}, ctx.Err()
	}
	sink.mu.Lock()
	output, overflow := append([]byte{}, sink.data...), sink.over
	sink.mu.Unlock()
	if overflow {
		return execution.Claim{}, execution.ErrLimit
	}
	if ctx.Err() != nil {
		return execution.Claim{}, ctx.Err()
	}
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		return execution.Claim{}, execution.ErrProvider
	}
	// A nonzero normal exit is a known completed check, not a transport failure.
	code := cmd.ProcessState.ExitCode()
	if code < 0 {
		return execution.Claim{}, execution.ErrProvider
	}
	r := Receipt{Version: 1, Task: j.Descriptor.TaskID, Operation: j.Descriptor.ID, Attempt: j.Request.AttemptID, Lease: j.Request.LeaseID, Generation: j.Generation, Config: w.config.Digest, ExitCode: code, OutputBytes: len(output), OutputHash: sum(output), InputsMatch: w.config.CheckInputs() == nil, Finished: time.Now().UTC()}
	w.mu.Lock()
	w.receipt, w.output = &r, output
	w.mu.Unlock()
	claim, err := json.Marshal(r)
	return execution.Claim{Output: string(claim), Units: 1}, err
}
