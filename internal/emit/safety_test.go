package emit

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNewRootRejectsBadInput(t *testing.T) {
	file := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		dir  string
		want error
	}{
		{"empty", "", ErrRootRelative},
		{"relative", "logs", ErrRootRelative},
		{"a file, not a directory", file, ErrRootNotDir},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRoot(tc.dir)
			if !errors.Is(err, tc.want) {
				t.Errorf("NewRoot(%q) error = %v, want %v", tc.dir, err, tc.want)
			}
		})
	}

	t.Run("missing directory", func(t *testing.T) {
		if _, err := NewRoot(filepath.Join(t.TempDir(), "nope")); err == nil {
			t.Error("NewRoot accepted a directory that does not exist")
		}
	})
}

// TestRootContainsWrites is the safety rail from PRD section 8.5. Every one of
// these inputs is a way an emitter could be talked into writing somewhere it
// has no business writing, and every one of them must abort.
func TestRootContainsWrites(t *testing.T) {
	dir := t.TempDir()
	root, err := NewRoot(dir)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("allowed", func(t *testing.T) {
		for _, name := range []string{"canary.log", "canary.log.1", "sub/nested.log"} {
			if name == "sub/nested.log" {
				if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			got, err := root.Path(name)
			if err != nil {
				t.Errorf("Path(%q) rejected a legitimate name: %v", name, err)
				continue
			}
			if !within(got, root.Dir()) {
				t.Errorf("Path(%q) = %q, which is outside the root", name, got)
			}
		}
	})

	t.Run("refused", func(t *testing.T) {
		cases := []string{
			"",
			"../escape.log",
			"../../escape.log",
			"sub/../../escape.log",
			"/etc/passwd",
			"/tmp/elsewhere.log",
		}
		for _, name := range cases {
			if got, err := root.Path(name); !errors.Is(err, ErrOutsideRoot) {
				t.Errorf("Path(%q) = %q, err = %v; want ErrOutsideRoot", name, got, err)
			}
		}
	})
}

// TestRootRefusesSymlinkEscape covers the case the resolution exists for: a
// symlink planted inside the root that points out of it.
func TestRootRefusesSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows, which this project does not target")
	}

	dir := t.TempDir()
	outside := t.TempDir()

	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Skipf("cannot create symlinks here: %v", err)
	}

	root, err := NewRoot(dir)
	if err != nil {
		t.Fatal(err)
	}

	if got, err := root.Path("escape/canary.log"); !errors.Is(err, ErrOutsideRoot) {
		t.Errorf("Path through a symlink = %q, err = %v; want ErrOutsideRoot", got, err)
	}
}

// TestRootDoesNotMatchSiblingPrefix guards the classic prefix bug where a root
// of /var/log would also accept /var/log-other.
func TestRootDoesNotMatchSiblingPrefix(t *testing.T) {
	base := t.TempDir()
	logs := filepath.Join(base, "log")
	other := filepath.Join(base, "log-other")
	for _, d := range []string{logs, other} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	root, err := NewRoot(logs)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := root.Path(filepath.Join(other, "canary.log")); !errors.Is(err, ErrOutsideRoot) {
		t.Errorf("sibling directory sharing a prefix was accepted: %q, err = %v", got, err)
	}
}

func TestCheckRate(t *testing.T) {
	cases := []struct {
		name    string
		rate    float64
		ack     bool
		wantErr bool
	}{
		{"default", 10, false, false},
		{"at the cap", SafeRateCap, false, false},
		{"over the cap", SafeRateCap + 1, false, true},
		{"over the cap, acknowledged", 10_000, true, false},
		{"zero", 0, false, true},
		{"negative", -1, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRate(tc.rate, tc.ack)
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckRate(%v, %v) error = %v, wantErr = %v", tc.rate, tc.ack, err, tc.wantErr)
			}
		})
	}
}
