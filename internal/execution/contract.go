// Package execution defines the R4 in-process worker/provider boundary. It does
// not authenticate network peers, schedule retries, or verify target state.
package execution

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

const Version = "1.0"
const MaxBytes = 64 * 1024
const MaxDuration = 5 * time.Minute
const MaxHeartbeats = 32

var (
	ErrInvalid     = errors.New("invalid execution contract")
	ErrUnsupported = errors.New("unsupported capability")
	ErrDenied      = errors.New("execution denied by policy")
	ErrUnavailable = errors.New("executor unavailable")
	ErrLimit       = errors.New("execution budget exceeded")
	ErrProvider    = errors.New("provider execution failed")
)

type Limits struct {
	Duration    time.Duration
	InputBytes  int
	OutputBytes int
	Units       int
}

func (l Limits) valid() bool {
	return l.Duration > 0 && l.Duration <= MaxDuration && l.InputBytes > 0 && l.InputBytes <= MaxBytes && l.OutputBytes > 0 && l.OutputBytes <= MaxBytes && l.Units > 0 && l.Units <= MaxBytes
}
func (l Limits) fits(max Limits) bool {
	return l.valid() && max.valid() && l.Duration <= max.Duration && l.InputBytes <= max.InputBytes && l.OutputBytes <= max.OutputBytes && l.Units <= max.Units
}

// Capabilities are availability, never authority. External describes whether
// the selected provider may receive data outside this local trust boundary.
type Capabilities struct {
	Version     string
	Worker      protocol.ActorID
	Provider    string
	Models      []string
	Actions     []string
	Effects     []protocol.EffectClass
	DataClasses []string
	External    bool
	Limits      Limits
}

type Request struct {
	Version       string
	MessageID     protocol.MessageID
	OperationID   protocol.OperationID
	AttemptID     protocol.AttemptID
	LeaseID       protocol.LeaseID
	RetryOf       protocol.AttemptID
	CorrelationID protocol.CorrelationID
	Revision      uint64
	IssuedAt      time.Time
	Deadline      time.Time // fixed lease upper bound; heartbeat cannot extend it
	Provider      string
	Model         string
	DataClass     string
	Input         string // minimized by caller; never stored in messages/evidence
	Limits        Limits
}

func (r Request) validate(now time.Time) error {
	if r.Version != Version {
		return ErrUnsupported
	}
	if r.MessageID == "" || r.OperationID == "" || r.AttemptID == "" || r.LeaseID == "" || r.CorrelationID == "" || r.Revision == 0 || r.IssuedAt.IsZero() || r.IssuedAt.After(now) || !r.Deadline.After(now) || r.Deadline.Sub(r.IssuedAt) > r.Limits.Duration || r.Provider == "" || r.Model == "" || r.DataClass == "" || !r.Limits.valid() {
		return ErrInvalid
	}
	if len(r.Input) > r.Limits.InputBytes {
		return ErrLimit
	}
	return nil
}

type Job struct {
	Request    Request
	Descriptor protocol.OperationDescriptor
	Worker     protocol.ActorID
	Generation uint64
}

type Claim struct {
	Output string
	Units  int
}

// Accept must be local, bounded, pure, and perform no provider I/O. Run must
// honor ctx and call heartbeat at bounded checkpoints. No method can verify an
// operation or access the controller's Store. Errors are mapped to fixed codes.
type Worker interface {
	Capabilities() Capabilities
	Accept(Job) error
	Run(context.Context, Job, func(context.Context) error) (Claim, error)
}

type ProviderRequest struct {
	Model  string
	Input  string
	Limits Limits
}

// Providers are explicitly selected by the composition root; no fallback API.
type Provider interface {
	ID() string
	Models() []string
	External() bool
	Generate(context.Context, ProviderRequest) (Claim, error)
}

// Policy is a trusted synchronous local lookup, not a model/prompt decision.
// It must be concurrency-safe and must not perform unbounded external I/O.
type Policy interface {
	Check(Job, Capabilities, time.Time) error
}

// Grant is supplied by trusted configuration, never by Request or provider.
// Exact matching intentionally has no wildcard or implicit inheritance.
type Grant struct {
	PolicyRef                                  string
	Worker                                     protocol.ActorID
	Provider, Model, Action, Target, DataClass string
	Effect                                     protocol.EffectClass
	AllowExternal                              bool
	Expires                                    time.Time
	Limits                                     Limits
}
type ExactPolicy struct{ grants []Grant }

func NewExactPolicy(grants []Grant) *ExactPolicy { return &ExactPolicy{grants: slices.Clone(grants)} }
func (p *ExactPolicy) Check(j Job, c Capabilities, now time.Time) error {
	for _, g := range p.grants {
		if g.PolicyRef == j.Descriptor.AuthorizationPolicyRef && g.Worker == j.Worker && g.Provider == j.Request.Provider && g.Model == j.Request.Model && g.Action == j.Descriptor.Name && g.Target == j.Descriptor.TargetRef && g.Effect == j.Descriptor.EffectClass && g.DataClass == j.Request.DataClass && (!c.External || g.AllowExternal) && now.Before(g.Expires) && !j.Request.Deadline.After(g.Expires) && j.Request.Limits.fits(g.Limits) {
			return nil
		}
	}
	return ErrDenied
}
func supports(c Capabilities, j Job) error {
	if c.Version != Version || c.Worker == "" || c.Worker != j.Worker || c.Provider != j.Request.Provider || !slices.Contains(c.Models, j.Request.Model) || !slices.Contains(c.Actions, j.Descriptor.Name) || !slices.Contains(c.Effects, j.Descriptor.EffectClass) || !slices.Contains(c.DataClasses, j.Request.DataClass) {
		return ErrUnsupported
	}
	if !j.Request.Limits.fits(c.Limits) {
		return ErrLimit
	}
	return nil
}
