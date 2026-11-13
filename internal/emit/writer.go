package emit

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Writer accepts the physical lines a profile rendered.
//
// WriteLines takes the whole group at once because a multi-line event must not
// be interleaved with another event's lines. Collectors reassemble stack traces
// by proximity, so splitting a group would manufacture the very corruption we
// are trying to measure.
type Writer interface {
	WriteLines(lines []string) error
	Rotate() error
	Sync() error
	Close() error
	Describe() string
}

// StdoutWriter writes to standard output, which is how a containerised
// application normally logs. The container runtime then owns the file on disk,
// so there is nothing for us to rotate.
type StdoutWriter struct {
	mu  sync.Mutex
	buf *bufio.Writer
	out io.Writer
}

// NewStdoutWriter wraps w, defaulting to os.Stdout.
func NewStdoutWriter(w io.Writer) *StdoutWriter {
	if w == nil {
		w = os.Stdout
	}
	return &StdoutWriter{out: w, buf: bufio.NewWriterSize(w, 64*1024)}
}

func (s *StdoutWriter) WriteLines(lines []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, line := range lines {
		if _, err := s.buf.WriteString(line); err != nil {
			return err
		}
		if err := s.buf.WriteByte('\n'); err != nil {
			return err
		}
	}
	// Flush per group. An unflushed canary that dies in our own buffer would
	// be counted as emitted and never delivered, which is a false accusation
	// against the pipeline, the one failure mode this project cannot have.
	return s.buf.Flush()
}

// Rotate is a no-op: the runtime owns rotation for stdout.
func (s *StdoutWriter) Rotate() error { return nil }

func (s *StdoutWriter) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Flush()
}

func (s *StdoutWriter) Close() error { return s.Sync() }

func (s *StdoutWriter) Describe() string { return "stdout" }

// FileConfig configures a self-rotating file writer.
type FileConfig struct {
	// Root bounds every path this writer may touch.
	Root *Root

	// Name is the active file's name inside the root, for example "canary.log".
	Name string

	// MaxBytes rotates the file once it reaches this size. Zero disables
	// size-based rotation.
	MaxBytes int64

	// MaxLines rotates after this many lines. Zero disables count-based
	// rotation. The rotation storm injector drives rotation through Rotate()
	// directly rather than through these thresholds.
	MaxLines int64

	// Keep is how many rotated generations to retain. Older ones are removed,
	// which is what lets a long soak run inside a small volume.
	Keep int

	// SyncEveryWrite calls fsync after each group. Slower, but it removes our
	// own page cache from the set of places a line can be lost, which matters
	// when the thing under test is whether lines get lost.
	SyncEveryWrite bool
}

// FileWriter writes canary lines to a file it rotates itself.
//
// Self-rotation is deliberate. Rotation races are one of the four structurally
// invisible loss classes (PRD section 2.1), and a harness that never rotates
// would never exercise them.
type FileWriter struct {
	cfg FileConfig

	mu        sync.Mutex
	file      *os.File
	buf       *bufio.Writer
	bytes     int64
	lines     int64
	rotations int64
	path      string
}

// NewFileWriter opens the active file, creating it if absent.
func NewFileWriter(cfg FileConfig) (*FileWriter, error) {
	if cfg.Root == nil {
		return nil, fmt.Errorf("emit: file writer needs a root")
	}
	if cfg.Name == "" {
		cfg.Name = "canary.log"
	}
	if cfg.Keep <= 0 {
		cfg.Keep = 5
	}

	path, err := cfg.Root.Path(cfg.Name)
	if err != nil {
		return nil, err
	}

	w := &FileWriter{cfg: cfg, path: path}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (f *FileWriter) open() error {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("emit: opening %s: %w", f.path, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("emit: inspecting %s: %w", f.path, err)
	}

	f.file = file
	f.buf = bufio.NewWriterSize(file, 64*1024)
	f.bytes = info.Size()
	f.lines = 0
	return nil
}

func (f *FileWriter) WriteLines(lines []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, line := range lines {
		n, err := f.buf.WriteString(line)
		if err != nil {
			return err
		}
		if err := f.buf.WriteByte('\n'); err != nil {
			return err
		}
		f.bytes += int64(n) + 1
		f.lines++
	}

	if err := f.buf.Flush(); err != nil {
		return err
	}
	if f.cfg.SyncEveryWrite {
		if err := f.file.Sync(); err != nil {
			return err
		}
	}

	// Threshold rotation happens only between groups, never inside one.
	if f.shouldRotate() {
		return f.rotateLocked()
	}
	return nil
}

func (f *FileWriter) shouldRotate() bool {
	if f.cfg.MaxBytes > 0 && f.bytes >= f.cfg.MaxBytes {
		return true
	}
	if f.cfg.MaxLines > 0 && f.lines >= f.cfg.MaxLines {
		return true
	}
	return false
}

// Rotate closes the active file, renames it to the next generation, and opens
// a fresh one. Callers use it to drive rotation explicitly.
func (f *FileWriter) Rotate() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rotateLocked()
}

// rotateLocked implements the logrotate convention: canary.log becomes
// canary.log.1, the old .1 becomes .2, and the generation past Keep is removed.
func (f *FileWriter) rotateLocked() error {
	if err := f.buf.Flush(); err != nil {
		return err
	}
	if err := f.file.Sync(); err != nil {
		return err
	}
	if err := f.file.Close(); err != nil {
		return err
	}

	for gen := f.cfg.Keep; gen >= 1; gen-- {
		older, err := f.generationPath(gen + 1)
		if err != nil {
			return err
		}
		current, err := f.generationPath(gen)
		if err != nil {
			return err
		}
		if _, err := os.Stat(current); err != nil {
			continue
		}
		if gen == f.cfg.Keep {
			if err := os.Remove(current); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("emit: removing %s: %w", current, err)
			}
			continue
		}
		if err := os.Rename(current, older); err != nil {
			return fmt.Errorf("emit: rotating %s to %s: %w", current, older, err)
		}
	}

	first, err := f.generationPath(1)
	if err != nil {
		return err
	}
	if err := os.Rename(f.path, first); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("emit: rotating %s to %s: %w", f.path, first, err)
	}

	f.rotations++
	return f.open()
}

// generationPath returns the path for rotation generation n, checked against
// the root so that a crafted Name cannot walk out through the suffix.
func (f *FileWriter) generationPath(n int) (string, error) {
	return f.cfg.Root.Path(fmt.Sprintf("%s.%d", f.cfg.Name, n))
}

func (f *FileWriter) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.buf.Flush(); err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *FileWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return nil
	}
	if err := f.buf.Flush(); err != nil {
		f.file.Close()
		return err
	}
	if err := f.file.Sync(); err != nil {
		f.file.Close()
		return err
	}
	err := f.file.Close()
	f.file = nil
	return err
}

// Rotations reports how many times this writer has rotated, which reports quote
// when explaining a rotation-race finding.
func (f *FileWriter) Rotations() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rotations
}

func (f *FileWriter) Describe() string {
	return "file:" + filepath.Base(f.path)
}

// Path returns the active file's absolute path.
func (f *FileWriter) Path() string { return f.path }
