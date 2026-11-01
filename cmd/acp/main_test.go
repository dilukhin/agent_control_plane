package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIChild(t *testing.T) {
	for i, arg := range os.Args {
		if arg == "--cli-helper" && i+1 < len(os.Args) {
			fmt.Print("local-only-output")
			if os.Args[i+1] == "fail" {
				os.Exit(9)
			}
			os.Exit(0)
		}
	}
}

func TestHelpAndInvalidArguments(t *testing.T) {
	for _, args := range [][]string{nil, {"--help"}, {"prepare", "--help"}, {"check", "--help"}, {"status", "--help"}, {"recover", "--help"}, {"gc", "--help"}} {
		var out, err bytes.Buffer
		if code := run(args, &out, &err); code != 0 {
			t.Fatalf("%v: %d %s", args, code, err.String())
		}
	}
	for _, args := range [][]string{{"unknown"}, {"check"}, {"status"}, {"recover"}, {"gc"}, {"status", "--state", "none", "--task", "../x"}, {"check", "--state", "none", "--task", "ok", "--program", "never-existing-acp-program", "--spec", "none", "--workspace", "none"}} {
		var out, err bytes.Buffer
		if code := run(args, &out, &err); code != 2 {
			t.Fatalf("%v: %d", args, code)
		}
	}
}

func TestCLIRealCommandStatusAndFailure(t *testing.T) {
	for _, mode := range []string{"ok", "fail"} {
		t.Run(mode, func(t *testing.T) {
			work := t.TempDir()
			source := []byte("pinned")
			os.WriteFile(filepath.Join(work, "input.txt"), source, 0600)
			profile := map[string]any{"version": 1, "label": "cli-check", "arguments": []string{"-test.run=^TestCLIChild$", "--", "--cli-helper", mode}, "timeout_seconds": 5, "max_output_bytes": 1024, "inputs": map[string]string{"input.txt": fmt.Sprintf("%x", sha256.Sum256(source))}}
			b, _ := json.Marshal(profile)
			spec := filepath.Join(t.TempDir(), "spec.json")
			os.WriteFile(spec, b, 0600)
			exe, _ := os.Executable()
			root := filepath.Join(t.TempDir(), "state")
			prepared := filepath.Join(t.TempDir(), "local.json")
			var prepOut, prepErr bytes.Buffer
			if code := run([]string{"prepare", "--spec", spec, "--program", exe, "--workspace", work, "--out", prepared}, &prepOut, &prepErr); code != 0 {
				t.Fatalf("prepare %d %s", code, prepErr.String())
			}
			spec = prepared
			args := []string{"check", "--state", root, "--task", "cli-check", "--spec", spec, "--workspace", work, "--program", exe}
			var out, err bytes.Buffer
			want := 0
			if mode == "fail" {
				want = 1
			}
			if code := run(args, &out, &err); code != want {
				t.Fatalf("%d %s %s", code, out.String(), err.String())
			}
			if strings.Contains(out.String()+err.String(), "local-only-output") {
				t.Fatal("implicit raw output")
			}
			var result map[string]any
			if e := json.Unmarshal(out.Bytes(), &result); e != nil {
				t.Fatal(e)
			}
			if result["exit_code"] != map[string]float64{"ok": 0, "fail": 9}[mode] {
				t.Fatal(result)
			}
			out.Reset()
			err.Reset()
			if code := run([]string{"status", "--state", root, "--task", "cli-check"}, &out, &err); code != want {
				t.Fatalf("status %d %s", code, err.String())
			}
			out.Reset()
			err.Reset()
			if code := run(args, &out, &err); code != 2 {
				t.Fatal("duplicate executed")
			}
		})
	}
}
