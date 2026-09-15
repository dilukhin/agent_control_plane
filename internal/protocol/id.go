package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

func NewOpaqueID(prefix string) (string, error) {
	if prefix == "" {
		return "", errors.New("id prefix is required")
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(raw[:]), nil
}

func NewTaskID() (TaskID, error) {
	v, err := NewOpaqueID("tsk")
	return TaskID(v), err
}

func NewOperationID() (OperationID, error) {
	v, err := NewOpaqueID("op")
	return OperationID(v), err
}

func NewAttemptID() (AttemptID, error) {
	v, err := NewOpaqueID("att")
	return AttemptID(v), err
}

func NewLeaseID() (LeaseID, error) {
	v, err := NewOpaqueID("lease")
	return LeaseID(v), err
}

func NewEvidenceID() (EvidenceID, error) {
	v, err := NewOpaqueID("ev")
	return EvidenceID(v), err
}
