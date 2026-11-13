package emit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Safety rail errors. Every one of these aborts rather than degrades. A tool
// that writes a canary into the wrong directory has done real damage to someone
// else's filesystem, and there is no partial-credit recovery from that.
var (
	ErrOutsideRoot  = errors.New("emit: refusing to write outside the configured directory")
	ErrRateTooHigh  = errors.New("emit: canary rate above the safe cap")
	ErrRootNotDir   = errors.New("emit: configured root is not a directory")
	ErrRootRelative = errors.New("emit: configured root must be an absolute path")
)

// SafeRateCap is the production ceiling on lines per second per source.
//
// Canary lines cost money in billed stores, and an emitter misconfigured by a
// factor of a thousand is a bill someone has to explain. Exceeding the cap
// requires an explicit acknowledgement flag (PRD section 8.5).
const SafeRateCap = 100.0

// Root is a directory the emitter is permitted to write inside, resolved
// through symlinks once at construction so that a link swapped in later cannot
// redirect a write.
type Root struct {
	configured string // what the operator wrote
	resolved   string // what it pointed at when we checked
}

// NewRoot resolves dir and verifies it is an existing absolute directory.
//
// Resolution happens here, once. Every later path check compares against the
// already-resolved value, so the window in which a symlink swap could redirect
// a write is the construction call rather than every write.
func NewRoot(dir string) (*Root, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty path", ErrRootRelative)
	}
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("%w: %s", ErrRootRelative, dir)
	}

	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, fmt.Errorf("emit: resolving root %s: %w", dir, err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("emit: inspecting root %s: %w", resolved, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrRootNotDir, resolved)
	}

	return &Root{configured: dir, resolved: filepath.Clean(resolved)}, nil
}

// Path returns the absolute path for name inside the root, or an error if the
// result would escape it.
//
// The check resolves whatever already exists along the path, so a relative
// traversal, an absolute path, or a symlink planted inside the root all fail
// the same way. A name for a file that does not exist yet is resolved through
// its parent directory, which does.
func (r *Root) Path(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: empty name", ErrOutsideRoot)
	}

	candidate := name
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(r.resolved, candidate)
	}
	candidate = filepath.Clean(candidate)

	// Resolve the deepest existing ancestor, then re-append the part that does
	// not exist yet. EvalSymlinks fails outright on a path whose leaf is
	// missing, which is the normal case for a log file about to be created.
	existing := candidate
	var trailing []string
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", fmt.Errorf("%w: %s has no existing ancestor", ErrOutsideRoot, candidate)
		}
		trailing = append([]string{filepath.Base(existing)}, trailing...)
		existing = parent
	}

	resolvedAncestor, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("emit: resolving %s: %w", existing, err)
	}
	final := filepath.Join(append([]string{resolvedAncestor}, trailing...)...)
	final = filepath.Clean(final)

	if !within(final, r.resolved) {
		return "", fmt.Errorf("%w: %s resolves to %s, outside %s",
			ErrOutsideRoot, name, final, r.resolved)
	}
	return final, nil
}

// within reports whether path is root itself or sits beneath it. Comparing with
// a trailing separator prevents /var/log-other matching root /var/log.
func within(path, root string) bool {
	if path == root {
		return true
	}
	prefix := root
	if !strings.HasSuffix(prefix, string(os.PathSeparator)) {
		prefix += string(os.PathSeparator)
	}
	return strings.HasPrefix(path, prefix)
}

// Dir returns the resolved root directory.
func (r *Root) Dir() string { return r.resolved }

// String renders the root for logs, showing the resolution when it differed
// from what the operator configured.
func (r *Root) String() string {
	if r.configured == r.resolved {
		return r.resolved
	}
	return fmt.Sprintf("%s (resolved from %s)", r.resolved, r.configured)
}

// CheckRate enforces the production rate cap. Passing acknowledged = true
// corresponds to the operator having set the override flag.
func CheckRate(linesPerSecond float64, acknowledged bool) error {
	if linesPerSecond <= 0 {
		return fmt.Errorf("emit: rate must be positive, got %v", linesPerSecond)
	}
	if linesPerSecond > SafeRateCap && !acknowledged {
		return fmt.Errorf("%w: %.1f lines/s exceeds the %.0f lines/s cap; pass the override flag if this is deliberate",
			ErrRateTooHigh, linesPerSecond, SafeRateCap)
	}
	return nil
}
