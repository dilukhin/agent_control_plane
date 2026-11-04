package reviewproof

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/core"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func gitAdapter(t *testing.T, ctx context.Context, root, mode string) []byte {
	t.Helper()
	scopedkb := os.Getenv("ACP_REVIEW_SCOPEDKB_ROOT")
	python := os.Getenv("ACP_REVIEW_PYTHON")
	if !filepath.IsAbs(scopedkb) || !filepath.IsAbs(python) {
		t.Fatal("ScopedKB source and Python must be explicitly pinned to absolute paths")
	}
	command := exec.CommandContext(ctx, python, "-B", "testdata/scopedkb_git.py", mode, root, scopedkb)
	command.Env = []string{"PYTHONPATH=" + filepath.Join(scopedkb, "src"), "PYTHONUTF8=1"}
	for _, name := range []string{"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP"} {
		if value, present := os.LookupEnv(name); present {
			command.Env = append(command.Env, name+"="+value)
		}
	}
	stdout, err := command.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("adapter %s: %v: %s", mode, err, exit.Stderr)
		}
		t.Fatalf("adapter %s: %v", mode, err)
	}
	return stdout
}

type gitObservation struct {
	Revision string
	Digest   string
	Main     string
	Parent   string
}

func TestGitAdapterAcrossProcesses(t *testing.T) {
	if os.Getenv("ACP_REVIEW_SCOPEDKB_ROOT") == "" {
		t.Skip("requires an explicitly selected local ScopedKB snapshot")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	root := t.TempDir()
	gitAdapter(t, ctx, root, "seed")
	var i intent
	must(t, json.Unmarshal(read(t, filepath.Join(root, "plan.json")), &i))
	if i.Version != 1 || len(i.BaseDigest) != 40 || !strings.HasPrefix(i.ResultDigest, "sha256:") {
		t.Fatal("unexpected integration plan")
	}

	executable, err := os.Executable()
	must(t, err)
	writerProcess := exec.CommandContext(ctx, executable, "-test.run=^TestWriterProcess$")
	writerProcess.Env = append(os.Environ(), "ACP_REVIEW_PROOF_ROOT="+root, "ACP_REVIEW_PROOF_MODE=git-adapter")
	out, err := writerProcess.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 42 {
		t.Fatalf("writer exit=%v output=%s", err, out)
	}

	store := openStore(t, ctx, root)
	defer store.Close()
	var confirmed []byte
	observe := func() (string, error) {
		confirmed = gitAdapter(t, ctx, root, "lookup")
		var observation gitObservation
		must(t, json.Unmarshal(confirmed, &observation))
		if observation.Parent != i.BaseDigest || observation.Main != i.BaseDigest ||
			observation.Revision == i.BaseDigest {
			return "", errors.New("proposal was not appended to the expected base")
		}
		return observation.Digest, nil
	}
	op := restore(t, ctx, store, i, observe)
	if op.State != state.OperationSucceeded || op.LeaseGeneration != 1 {
		t.Fatalf("recovered=%s generation=%d", op.State, op.LeaseGeneration)
	}
	again := restore(t, ctx, store, i, observe)
	if again.Revision != op.Revision {
		t.Fatal("recovery changed the verified operation")
	}
	if after := gitAdapter(t, ctx, root, "lookup"); !bytes.Equal(after, confirmed) {
		t.Fatal("proposal revision or content changed after verification")
	}
	if err := store.CheckActiveAttemptContext(ctx, opID, attemptID, "review-lease", 1, writer); !errors.Is(err, core.ErrStaleOwnership) {
		t.Fatalf("old authority returned: %v", err)
	}
}
