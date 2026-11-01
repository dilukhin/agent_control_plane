package execution

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

// Local is the reference read-only analyzer. No shell, filesystem write,
// network client, tool execution, or credential API is exposed to providers.
type Local struct {
	actor    protocol.ActorID
	provider Provider
}

func NewLocal(actor protocol.ActorID, provider Provider) (*Local, error) {
	if actor == "" || provider == nil {
		return nil, ErrInvalid
	}
	return &Local{actor: actor, provider: provider}, nil
}
func (l *Local) Capabilities() Capabilities {
	return Capabilities{Version: Version, Worker: l.actor, Provider: l.provider.ID(), Models: slices.Clone(l.provider.Models()), Actions: []string{"analyze"}, Effects: []protocol.EffectClass{protocol.EffectReadOnly}, DataClasses: []string{"synthetic", "public", "private"}, External: l.provider.External(), Limits: Limits{MaxDuration, MaxBytes, MaxBytes, MaxBytes}}
}
func (l *Local) Accept(j Job) error { return supports(l.Capabilities(), j) }
func (l *Local) Run(ctx context.Context, j Job, heartbeat func(context.Context) error) (Claim, error) {
	if err := l.Accept(j); err != nil {
		return Claim{}, err
	}
	if err := heartbeat(ctx); err != nil {
		return Claim{}, err
	}
	return l.provider.Generate(ctx, ProviderRequest{j.Request.Model, j.Request.Input, j.Request.Limits})
}

// EchoProvider and DigestProvider are deterministic, zero-cost test providers.
// Each performs one bounded call; Units counts input bytes, not billed tokens.
type EchoProvider struct{}

func (EchoProvider) ID() string       { return "test.echo" }
func (EchoProvider) Models() []string { return []string{"echo-v1"} }
func (EchoProvider) External() bool   { return false }
func (EchoProvider) Generate(ctx context.Context, r ProviderRequest) (Claim, error) {
	if err := providerCheck(ctx, r, "echo-v1"); err != nil {
		return Claim{}, err
	}
	if len(r.Input) > r.Limits.OutputBytes {
		return Claim{}, ErrLimit
	}
	return Claim{r.Input, len(r.Input)}, nil
}

type DigestProvider struct{}

func (DigestProvider) ID() string       { return "test.digest" }
func (DigestProvider) Models() []string { return []string{"sha256-v1"} }
func (DigestProvider) External() bool   { return false }
func (DigestProvider) Generate(ctx context.Context, r ProviderRequest) (Claim, error) {
	if err := providerCheck(ctx, r, "sha256-v1"); err != nil {
		return Claim{}, err
	}
	out := fmt.Sprintf("%x", sha256.Sum256([]byte(r.Input)))
	if len(out) > r.Limits.OutputBytes {
		return Claim{}, ErrLimit
	}
	return Claim{out, len(r.Input)}, nil
}
func providerCheck(ctx context.Context, r ProviderRequest, model string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Model != model {
		return ErrUnsupported
	}
	if !r.Limits.valid() || len(r.Input) > r.Limits.InputBytes || len(r.Input) > r.Limits.Units {
		return ErrLimit
	}
	return nil
}
