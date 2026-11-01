package process

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dilukhin/agent_control_plane/internal/execution"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

const cleanupBudget = 2 * time.Second

// Config is trusted local composition, never derived from a job or model.
// FixedArgs is for fixed launch modes, not command text. No shell is invoked.
type Config struct {
	Executable  string
	FixedArgs   []string
	MaxParallel int
}

// Stats is bounded transient transport diagnostics, not durable task state or
// proof of result. It contains no input, output, argv, environment or paths.
type Stats struct {
	Started, Reaped, Heartbeats uint64
	Active, LastPID             int
}

type Worker struct {
	config Config
	local  *execution.Local
	slots  chan struct{}
	mu     sync.Mutex
	stats  Stats
	idle   chan struct{}
}

func executableOK(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	s, err := os.Lstat(path)
	if err != nil || !s.Mode().IsRegular() {
		return false
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(filepath.Ext(path), ".exe")
	}
	return s.Mode().Perm()&0111 != 0
}

func New(c Config, actor protocol.ActorID, providerID string) (*Worker, error) {
	if len(actor) > MaxIdentifier || !utf8.ValidString(string(actor)) || len(c.FixedArgs) > 16 {
		return nil, execution.ErrLimit
	}
	for _, arg := range c.FixedArgs {
		if len(arg) > 4096 || !utf8.ValidString(arg) {
			return nil, execution.ErrLimit
		}
	}
	p, err := provider(providerID)
	if err != nil {
		return nil, err
	}
	local, err := execution.NewLocal(actor, p)
	if err != nil || !executableOK(c.Executable) {
		return nil, execution.ErrInvalid
	}
	if c.MaxParallel == 0 {
		c.MaxParallel = 4
	}
	if c.MaxParallel < 1 || c.MaxParallel > 32 {
		return nil, execution.ErrLimit
	}
	c.FixedArgs = slices.Clone(c.FixedArgs)
	idle := make(chan struct{})
	close(idle)
	return &Worker{config: c, local: local, slots: make(chan struct{}, c.MaxParallel), idle: idle}, nil
}

func (w *Worker) Capabilities() execution.Capabilities {
	c := w.local.Capabilities()
	c.DataClasses = []string{"synthetic"}
	return c
}

func (w *Worker) Accept(j execution.Job) error {
	if !utf8.ValidString(j.Request.Input) {
		return execution.ErrInvalid
	}
	if !boundedIdentity(j) {
		return execution.ErrLimit
	}
	if j.Request.Version != execution.Version || j.Request.DataClass != "synthetic" {
		return execution.ErrUnsupported
	}
	if err := j.Descriptor.Validate(); err != nil {
		return execution.ErrInvalid
	}
	return w.local.Accept(j)
}

func (w *Worker) Snapshot() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// WaitIdle is a local cleanup barrier when the trusted caller has stopped new
// submissions. It neither revokes attempts nor authorizes retries.
func (w *Worker) WaitIdle(ctx context.Context) error {
	w.mu.Lock()
	idle := w.idle
	w.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) acquire() bool {
	select {
	case w.slots <- struct{}{}:
	default:
		return false
	}
	w.mu.Lock()
	if w.stats.Active == 0 {
		w.idle = make(chan struct{})
	}
	w.stats.Active++
	w.mu.Unlock()
	return true
}

func (w *Worker) release() {
	w.mu.Lock()
	w.stats.Active--
	if w.stats.Active == 0 {
		close(w.idle)
	}
	w.mu.Unlock()
	<-w.slots
}

type stderrSink struct {
	mu     sync.Mutex
	n      int
	over   bool
	cancel context.CancelFunc
}

func (s *stderrSink) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.over {
		if len(b) > MaxStderr-s.n {
			s.over = true
			s.cancel()
		} else {
			s.n += len(b)
		}
	}
	return len(b), nil // drain without retaining or exposing diagnostic bytes
}

func (s *stderrSink) exceeded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.over
}

func childEnv() []string {
	result := make([]string, 0, 4) // non-nil: nil means inherit all of os.Environ
	// Required Windows runtime/temp locations only. Do not inherit credentials,
	// proxy settings, provider tokens, HOME, PATH or arbitrary caller environment.
	for _, key := range []string{"SYSTEMROOT", "WINDIR", "TEMP", "TMP"} {
		if value, ok := os.LookupEnv(key); ok {
			result = append(result, key+"="+value)
		}
	}
	return result
}

func (w *Worker) exchange(ctx context.Context, j execution.Job, input io.WriteCloser, output io.Reader, pid int, heartbeat func(context.Context) error) (execution.Claim, error) {
	if err := writeFrame(input, frame{Version: WireVersion, Kind: "job", Binding: bind(j), Job: &j}); err != nil {
		return execution.Claim{}, err
	}
	beats := 0
	for {
		f, err := readFrame(output)
		if err != nil {
			return execution.Claim{}, ErrWire
		}
		if f.Binding != bind(j) || f.PID != pid || f.Job != nil {
			return execution.Claim{}, ErrWire
		}
		switch f.Kind {
		case "heartbeat":
			if f.Claim != nil || f.Reason != "" || beats >= execution.MaxHeartbeats {
				return execution.Claim{}, ErrWire
			}
			beats++
			if err := heartbeat(ctx); err != nil {
				return execution.Claim{}, err
			}
			if err := writeFrame(input, frame{Version: WireVersion, Kind: "continue", Binding: bind(j)}); err != nil {
				return execution.Claim{}, err
			}
			w.mu.Lock()
			w.stats.Heartbeats++
			w.mu.Unlock()
		case "result":
			if f.Claim == nil || f.Reason != "" {
				return execution.Claim{}, ErrWire
			}
			if len(f.Claim.Output) > j.Request.Limits.OutputBytes || f.Claim.Units < 0 || f.Claim.Units > j.Request.Limits.Units {
				return execution.Claim{}, execution.ErrLimit
			}
			// A result is accepted only after clean EOF and successful process
			// exit. A second result, garbage, or a truncated frame is not success.
			if _, err := readFrame(output); err != io.EOF {
				return execution.Claim{}, ErrWire
			}
			return *f.Claim, nil
		case "error":
			if f.Claim != nil {
				return execution.Claim{}, ErrWire
			}
			switch f.Reason {
			case "provider_failed":
				return execution.Claim{}, execution.ErrProvider
			case "budget_exceeded":
				return execution.Claim{}, execution.ErrLimit
			case "unsupported":
				return execution.Claim{}, execution.ErrUnsupported
			case "timeout":
				return execution.Claim{}, context.DeadlineExceeded
			case "cancelled":
				return execution.Claim{}, context.Canceled
			default:
				return execution.Claim{}, ErrWire
			}
		default:
			return execution.Claim{}, ErrWire
		}
	}
}

// Run launches exactly one owned process. The surrounding Runner is the sole
// owner of admission, deduplication, state transitions and result verification.
func (w *Worker) Run(ctx context.Context, j execution.Job, heartbeat func(context.Context) error) (claim execution.Claim, err error) {
	if err := ctx.Err(); err != nil {
		return claim, err
	}
	if heartbeat == nil {
		return claim, execution.ErrInvalid
	}
	if err := validJob(j, w.local); err != nil {
		return claim, err
	}
	if !w.acquire() {
		return claim, execution.ErrUnavailable
	}
	releaseHere := true
	defer func() {
		if releaseHere {
			w.release()
		}
	}()
	ctx, cancel := context.WithDeadline(ctx, j.Request.Deadline)
	defer cancel()
	if !executableOK(w.config.Executable) {
		return claim, execution.ErrUnavailable
	}
	inR, inW, e := os.Pipe()
	if e != nil {
		return claim, execution.ErrUnavailable
	}
	defer inR.Close()
	defer inW.Close()
	outR, outW, e := os.Pipe()
	if e != nil {
		return claim, execution.ErrUnavailable
	}
	defer outR.Close()
	defer outW.Close()
	args := append(slices.Clone(w.config.FixedArgs), "--acp-worker-v1", string(j.Worker), j.Request.Provider)
	cmd := exec.CommandContext(ctx, w.config.Executable, args...)
	cmd.Env = childEnv()
	cmd.Stdin, cmd.Stdout = inR, outW
	sink := &stderrSink{cancel: cancel}
	cmd.Stderr = sink
	cmd.WaitDelay = cleanupBudget
	if e := cmd.Start(); e != nil {
		return claim, execution.ErrUnavailable
	}
	inR.Close()
	outW.Close() // only child owns these ends now
	w.mu.Lock()
	w.stats.Started++
	w.stats.LastPID = cmd.Process.Pid
	w.mu.Unlock()
	var waitErr error
	waitDone := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		w.mu.Lock()
		if cmd.ProcessState != nil {
			w.stats.Reaped++
		}
		w.mu.Unlock()
		close(waitDone)
	}()
	var ioClaim execution.Claim
	var ioErr error
	ioDone := make(chan struct{})
	go func() {
		defer close(ioDone)
		defer func() {
			if recover() != nil {
				ioErr = execution.ErrProvider
			}
		}()
		ioClaim, ioErr = w.exchange(ctx, j, inW, outR, cmd.Process.Pid, heartbeat)
	}()
	// The slot is retained until both owned process and pipe handling finish,
	// even if the bounded cleanup wait expires. Thus failures cannot create
	// unbounded abandoned goroutines/processes through repeated submissions.
	allDone := make(chan struct{})
	go func() { <-waitDone; <-ioDone; close(allDone) }()
	select {
	case <-ioDone:
		claim, err = ioClaim, ioErr
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil {
		select {
		case <-waitDone:
			if waitErr != nil {
				err = execution.ErrProvider
			}
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if sink.exceeded() {
		err = execution.ErrLimit
	} else if ctx.Err() != nil {
		err = ctx.Err()
	}
	cancel()
	inW.Close()
	outR.Close()
	timer := time.NewTimer(cleanupBudget)
	defer timer.Stop()
	select {
	case <-allDone:
	case <-timer.C:
		releaseHere = false
		go func() { <-allDone; w.release() }()
		err = errors.Join(err, ErrCleanup)
	}
	if err != nil {
		return execution.Claim{}, err
	}
	return claim, nil
}
