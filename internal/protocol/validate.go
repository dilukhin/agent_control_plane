package protocol

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidProtocolVersion = errors.New("invalid protocol version")
	ErrInvalidDescriptor      = errors.New("invalid operation descriptor")
	ErrSensitiveField         = errors.New("sensitive field is not allowed")
	ErrInvalidPayloadValue   = errors.New("invalid canonical payload value")
	ErrUnsupportedMessage     = errors.New("unsupported message")
)

var supportedMessageNames = map[string]struct{}{
	"operation.offer":               {},
	"operation.cancel_request":      {},
	"operation.accepted":            {},
	"operation.rejected":            {},
	"operation.started":             {},
	"operation.progress":            {},
	"operation.result_reported":     {},
	"operation.execution_uncertain": {},
	"verification.observed":         {},
	"verification.completed":        {},
	"issue.opened":                  {},
	"issue.updated":                 {},
	"issue.closed":                  {},
	"escalation.requested":          {},
	"escalation.resolved":           {},
}

func (d OperationDescriptor) Validate() error {
	if d.ID == "" || d.TaskID == "" || strings.TrimSpace(d.Name) == "" || strings.TrimSpace(d.TargetRef) == "" {
		return fmt.Errorf("%w: identity, name, and target are required", ErrInvalidDescriptor)
	}
	switch d.EffectClass {
	case EffectReadOnly, EffectMutation:
	default:
		return fmt.Errorf("%w: unknown effect_class %q", ErrInvalidDescriptor, d.EffectClass)
	}
	switch d.IdempotencyMode {
	case IdempotencyUnknown, IdempotencyIdempotent, IdempotencyConditional:
	default:
		return fmt.Errorf("%w: unknown idempotency_mode %q", ErrInvalidDescriptor, d.IdempotencyMode)
	}
	if strings.TrimSpace(d.VerificationPolicyRef) == "" || strings.TrimSpace(d.AuthorizationPolicyRef) == "" {
		return fmt.Errorf("%w: verification and authorization policies are required", ErrInvalidDescriptor)
	}
	seen := make(map[string]struct{}, len(d.ConflictScope))
	for _, scope := range d.ConflictScope {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			return fmt.Errorf("%w: empty conflict scope", ErrInvalidDescriptor)
		}
		if _, ok := seen[scope]; ok {
			return fmt.Errorf("%w: duplicate conflict scope %q", ErrInvalidDescriptor, scope)
		}
		seen[scope] = struct{}{}
	}
	if d.EffectClass == EffectMutation && len(d.ConflictScope) == 0 {
		return fmt.Errorf("%w: mutation requires conflict scope", ErrInvalidDescriptor)
	}
	return nil
}

func (e Envelope) Validate() error {
	if e.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("%w: got %q want %q", ErrInvalidProtocolVersion, e.ProtocolVersion, ProtocolVersion)
	}
	if e.MessageID == "" || e.TaskID == "" || e.CorrelationID == "" || e.Actor.ID == "" || strings.TrimSpace(e.Actor.Role) == "" {
		return errors.New("message identity, task, correlation, and actor are required")
	}
	switch e.MessageType {
	case MessageCommand, MessageEvent, MessageObservation, MessageDecisionRequest, MessageDecisionResponse:
	default:
		return fmt.Errorf("unknown message_type %q", e.MessageType)
	}
	if strings.TrimSpace(e.MessageName) == "" || e.IssuedAt.IsZero() {
		return errors.New("message_name and issued_at are required")
	}
	if _, ok := supportedMessageNames[e.MessageName]; !ok {
		return fmt.Errorf("%w: %s", ErrUnsupportedMessage, e.MessageName)
	}
	if (strings.HasPrefix(e.MessageName, "operation.") || strings.HasPrefix(e.MessageName, "verification.")) && e.OperationID == "" {
		return errors.New("operation_id is required for operation and verification messages")
	}
	if err := ValidateNoSensitiveFields(e.Payload); err != nil {
		return err
	}
	if err := ValidateNoSensitiveFields(e.Extensions); err != nil {
		return err
	}
	return nil
}

func ValidateNoSensitiveFields(v any) error {
	switch x := v.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return nil
	case map[string]any:
		for key, value := range x {
			if isForbiddenKey(key) {
				return fmt.Errorf("%w: %s", ErrSensitiveField, key)
			}
			if err := ValidateNoSensitiveFields(value); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for _, value := range x {
			if err := ValidateNoSensitiveFields(value); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: %T", ErrInvalidPayloadValue, v)
	}
}

func isForbiddenKey(key string) bool {
	n := strings.ToLower(strings.TrimSpace(key))
	n = strings.ReplaceAll(n, "-", "_")
	if n == "secret_ref" {
		return false
	}
	for _, fragment := range []string{
		"password",
		"passphrase",
		"private_key",
		"authorization",
		"cookie",
		"credential",
		"access_token",
		"api_token",
		"session_token",
		"relay_token",
		"secret",
	} {
		if strings.Contains(n, fragment) {
			return true
		}
	}
	return false
}
