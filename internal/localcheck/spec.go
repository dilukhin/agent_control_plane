// Package localcheck composes the existing core for trusted local project checks.
// A spec is operator configuration, never model output or an authorization service.
package localcheck

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

var ErrConfig = errors.New("неверная конфигурация местной проверки")
var ErrInputs = errors.New("исходные файлы не совпадают с выбранным снимком")
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Spec struct {
	Version        int               `json:"version"`
	Label          string            `json:"label"`
	Arguments      []string          `json:"arguments"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	MaxOutputBytes int               `json:"max_output_bytes"`
	Inputs         map[string]string `json:"inputs"`
}

type Config struct {
	Spec       Spec
	Program    string
	Workspace  string
	Digest     string
	programSum string
}

func sum(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

func decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return ErrConfig
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrConfig
	}
	return nil
}

func regular(path string) (os.FileInfo, error) {
	s, err := os.Lstat(path)
	if err != nil || !s.Mode().IsRegular() {
		return nil, ErrConfig
	}
	return s, nil
}

// readInput refuses symlinks in every component, traversal, and oversized files.
// It is not a sandbox against a concurrent actor with the same OS identity.
func readInput(root, rel string) ([]byte, error) {
	if rel == "" || strings.Contains(rel, "\\") || strings.Contains(rel, ":") || strings.HasPrefix(rel, "/") || strings.ContainsRune(rel, 0) {
		return nil, ErrInputs
	}
	parts := strings.Split(rel, "/")
	path := root
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, ErrInputs
		}
		path = filepath.Join(path, part)
		s, err := os.Lstat(path)
		if err != nil || s.Mode()&os.ModeSymlink != 0 || (i < len(parts)-1 && !s.IsDir()) || (i == len(parts)-1 && (!s.Mode().IsRegular() || s.Size() > 4*1024*1024)) {
			return nil, ErrInputs
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrInputs
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4*1024*1024+1))
	if err != nil || len(b) > 4*1024*1024 {
		return nil, ErrInputs
	}
	return b, nil
}

func (c Config) CheckInputs() error {
	total := 0
	for path, expected := range c.Spec.Inputs {
		b, err := readInput(c.Workspace, path)
		if err != nil || sum(b) != expected {
			return ErrInputs
		}
		total += len(b)
		if total > 16*1024*1024 {
			return ErrInputs
		}
	}
	return nil
}

// Load pins the selected input bytes and executable. Paths and argv stay local;
// only the digest and safe label are recorded in canonical task evidence.
func Load(specPath, program, workspace string) (Config, error) {
	var c Config
	f, err := os.Open(specPath)
	if err != nil {
		return c, ErrConfig
	}
	b, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	f.Close()
	if err != nil || len(b) > 64*1024 || decode(b, &c.Spec) != nil {
		return c, ErrConfig
	}
	s := c.Spec
	if s.Version != 1 || !idPattern.MatchString(s.Label) || s.TimeoutSeconds < 1 || s.TimeoutSeconds > 300 || s.MaxOutputBytes < 1 || s.MaxOutputBytes > 64*1024 || len(s.Arguments) > 64 || len(s.Inputs) < 1 || len(s.Inputs) > 128 {
		return c, ErrConfig
	}
	argsSize := 0
	for _, arg := range s.Arguments {
		if len(arg) > 4096 || strings.ContainsRune(arg, 0) {
			return c, ErrConfig
		}
		argsSize += len(arg)
	}
	if argsSize > 16*1024 {
		return c, ErrConfig
	}
	for _, h := range s.Inputs {
		if !hashPattern.MatchString(h) {
			return c, ErrConfig
		}
	}
	c.Program, err = filepath.Abs(program)
	if err != nil {
		return c, ErrConfig
	}
	c.Workspace, err = filepath.Abs(workspace)
	if err != nil {
		return c, ErrConfig
	}
	c.Workspace, err = filepath.EvalSymlinks(c.Workspace)
	if err != nil {
		return c, ErrConfig
	}
	c.Program, err = filepath.EvalSymlinks(c.Program)
	if err != nil {
		return c, ErrConfig
	}
	if runtime.GOOS == "windows" {
		c.Workspace = strings.ToLower(c.Workspace)
	}
	st, err := os.Lstat(c.Workspace)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return c, ErrConfig
	}
	info, err := regular(c.Program)
	if err != nil || info.Size() > 256*1024*1024 {
		return c, ErrConfig
	}
	exe, err := os.ReadFile(c.Program)
	if err != nil {
		return c, ErrConfig
	}
	c.programSum = sum(exe)
	canonical, err := json.Marshal(s)
	if err != nil {
		return c, ErrConfig
	}
	c.Digest = sum(append(append(canonical, 0), []byte(c.programSum+"\x00"+c.Workspace+"\x00"+c.Program)...))
	if err := c.CheckInputs(); err != nil {
		return c, err
	}
	return c, nil
}

func (c Config) duration() time.Duration { return time.Duration(c.Spec.TimeoutSeconds) * time.Second }

func (c Config) clone() Config {
	c.Spec.Arguments = append([]string{}, c.Spec.Arguments...)
	inputs := map[string]string{}
	keys := make([]string, 0, len(c.Spec.Inputs))
	for k := range c.Spec.Inputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		inputs[k] = c.Spec.Inputs[k]
	}
	c.Spec.Inputs = inputs
	return c
}

func validDigest(v string) bool {
	_, err := hex.DecodeString(v)
	return len(v) == 64 && err == nil
}
