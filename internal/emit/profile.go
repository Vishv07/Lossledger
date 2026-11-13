// Package emit produces canary log lines through the same paths a real
// application uses: files that rotate, stdout, and (post-v1) the journald
// socket. It is deliberately only another log producer. Nothing here reaches
// into the pipeline under test.
package emit

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/Vishv07/lossledger/internal/wire"
)

// Kind is the structural family a profile belongs to. The verifier does not
// care which profile produced a line, but the loss does: a 64 KB line and a
// 40-line stack trace fail in different places than a tidy one-liner, which is
// the whole reason shape profiles exist (PRD section 7.2).
type Kind string

const (
	KindSingle     Kind = "single"     // one physical line, token embedded in text
	KindJSON       Kind = "json"       // one physical line of JSON, token in a field
	KindMultiline  Kind = "multiline"  // token on line 1, continuation lines after
	KindOversize   Kind = "oversize"   // one very long physical line
	KindInterleave Kind = "interleave" // token line plus a deliberately back-dated neighbour
)

// Profile describes one realistic line shape.
//
// Profiles are data: a contributor adds coverage by writing a definition, never
// by touching the emitter (PRD principle 4). The Go struct is the schema; the
// files in profiles/ are instances of it.
type Profile struct {
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
	Kind        Kind   `yaml:"kind" json:"kind"`

	// Prefix is written ahead of the token on the first physical line, so a
	// profile can imitate a logger's own preamble.
	Prefix string `yaml:"prefix" json:"prefix"`

	// Suffix is written after the token on the first physical line.
	Suffix string `yaml:"suffix" json:"suffix"`

	// MinLines and MaxLines bound the continuation lines of a multiline
	// profile. The count is drawn from the run's seeded source, so a replay
	// reproduces the same shapes.
	MinLines int `yaml:"min_lines" json:"min_lines"`
	MaxLines int `yaml:"max_lines" json:"max_lines"`

	// SizeBytes is the target physical line length for an oversize profile.
	SizeBytes int `yaml:"size_bytes" json:"size_bytes"`

	// BackdateNanos is how far behind the token's own timestamp an interleave
	// profile stamps its neighbour line. Out-of-order timestamps are what
	// trigger the silent-drop class documented in grafana/alloy issue 7076.
	BackdateNanos int64 `yaml:"backdate_nanos" json:"backdate_nanos"`
}

// Builtins are the normative v1 profiles. M1 gates on plain, json and
// stacktrace; oversize and interleave land in M2 but are defined here so the
// schema is exercised by all five from the start.
func Builtins() []Profile {
	return []Profile{
		{
			Name:        "plain",
			Description: "A single unstructured line, the shape most tutorials assume.",
			Kind:        KindSingle,
			Prefix:      "INFO  canary ",
		},
		{
			Name:        "json",
			Description: "One JSON object per line with the token in a message field.",
			Kind:        KindJSON,
		},
		{
			Name:        "stacktrace",
			Description: "A multi-line exception. The token sits on the first physical line; continuation lines carry LLC markers so partial delivery is detectable.",
			Kind:        KindMultiline,
			Prefix:      "ERROR canary java.lang.IllegalStateException: ",
			MinLines:    8,
			MaxLines:    40,
		},
		{
			Name:        "oversize",
			Description: "A single very long line, past the point where collectors and stores start truncating or rejecting.",
			Kind:        KindOversize,
			Prefix:      "WARN  canary oversize ",
			SizeBytes:   64 * 1024,
		},
		{
			Name:          "interleave",
			Description:   "Two streams writing alternately with deliberately non-monotonic timestamps. This is the alloy 7076 trigger.",
			Kind:          KindInterleave,
			Prefix:        "INFO  canary ",
			BackdateNanos: int64(2 * 1000 * 1000 * 1000),
		},
	}
}

// Validate reports whether the profile is internally coherent. Loading an
// invalid profile is a configuration error, caught before a run starts rather
// than halfway through one.
func (p Profile) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("emit: profile has no name")
	}
	switch p.Kind {
	case KindSingle, KindJSON, KindInterleave:
		// No extra constraints.
	case KindMultiline:
		if p.MinLines < 1 || p.MaxLines < p.MinLines {
			return fmt.Errorf("emit: profile %q: min_lines must be at least 1 and max_lines at least min_lines, got %d and %d",
				p.Name, p.MinLines, p.MaxLines)
		}
	case KindOversize:
		if p.SizeBytes < 1024 {
			return fmt.Errorf("emit: profile %q: size_bytes must be at least 1024, got %d", p.Name, p.SizeBytes)
		}
	default:
		return fmt.Errorf("emit: profile %q: unknown kind %q", p.Name, p.Kind)
	}
	return nil
}

// Render turns one token into the physical lines that get written.
//
// The returned slice is always non-empty and its first element always contains
// the canary token contiguously. Randomness comes only from rnd, so a seeded
// run renders byte-identically on replay.
func (p Profile) Render(tok wire.Token, key []byte, rnd *rand.Rand) []string {
	tok.Shape = p.Name
	token := tok.Format(key)

	switch p.Kind {
	case KindJSON:
		// Marshal rather than concatenate so that a token containing a quote
		// or a backslash cannot produce invalid JSON.
		obj := map[string]any{
			"ts":     tok.TS,
			"level":  "info",
			"logger": "lossledger.canary",
			"msg":    p.Prefix + token + p.Suffix,
		}
		encoded, err := json.Marshal(obj)
		if err != nil {
			// A map of strings and ints cannot fail to marshal; fall back to
			// the single-line shape rather than dropping the sequence.
			return []string{p.Prefix + token + p.Suffix}
		}
		return []string{string(encoded)}

	case KindMultiline:
		n := p.MinLines
		if p.MaxLines > p.MinLines {
			n += rnd.Intn(p.MaxLines - p.MinLines + 1)
		}
		lines := make([]string, 0, n+1)
		lines = append(lines, p.Prefix+token+p.Suffix)
		for i := 1; i <= n; i++ {
			marker := wire.Token{
				Kind:      wire.KindContinuation,
				Src:       tok.Src,
				Epoch:     tok.Epoch,
				Seq:       tok.Seq,
				ContIndex: i,
				ContTotal: n,
			}
			lines = append(lines, fmt.Sprintf("\tat com.example.service.Layer%d.call(Layer%d.java:%d) %s",
				i, i, 100+i, marker.Format(key)))
		}
		return lines

	case KindOversize:
		head := p.Prefix + token + " "
		if len(head) >= p.SizeBytes {
			return []string{head}
		}
		// Pad with a repeating pattern rather than a single character so a
		// truncation is visible in the padding itself.
		var b strings.Builder
		b.Grow(p.SizeBytes)
		b.WriteString(head)
		const filler = "0123456789abcdef"
		for b.Len() < p.SizeBytes {
			remaining := p.SizeBytes - b.Len()
			if remaining >= len(filler) {
				b.WriteString(filler)
			} else {
				b.WriteString(filler[:remaining])
			}
		}
		return []string{b.String()}

	case KindInterleave:
		// The neighbour line is stamped in the past and carries no token. Its
		// only job is to make the stream's timestamps non-monotonic, which is
		// what makes some collectors drop the line that follows.
		backdated := tok.TS - p.BackdateNanos
		return []string{
			fmt.Sprintf("INFO  canary neighbour ts=%d (deliberately out of order)", backdated),
			p.Prefix + token + p.Suffix,
		}

	default: // KindSingle
		return []string{p.Prefix + token + p.Suffix}
	}
}

// Set is a name-indexed collection of profiles plus the weights used to choose
// among them.
type Set struct {
	profiles map[string]Profile
	order    []string
}

// NewSet indexes profiles by name, rejecting duplicates and invalid entries.
func NewSet(profiles []Profile) (*Set, error) {
	s := &Set{profiles: make(map[string]Profile, len(profiles))}
	for _, p := range profiles {
		if err := p.Validate(); err != nil {
			return nil, err
		}
		if _, dup := s.profiles[p.Name]; dup {
			return nil, fmt.Errorf("emit: duplicate profile name %q", p.Name)
		}
		s.profiles[p.Name] = p
		s.order = append(s.order, p.Name)
	}
	return s, nil
}

// Get returns the named profile.
func (s *Set) Get(name string) (Profile, bool) {
	p, ok := s.profiles[name]
	return p, ok
}

// Names lists profile names in definition order.
func (s *Set) Names() []string {
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out
}

// Mix chooses profiles by weight. Weights need not sum to 1; they are
// normalised. Selection draws from the run's seeded source so a replayed run
// picks the same shape for the same sequence number.
type Mix struct {
	names   []string
	cumul   []float64
	total   float64
	profile []Profile
}

// NewMix builds a weighted chooser over the named profiles.
func NewMix(set *Set, weights map[string]float64) (*Mix, error) {
	if len(weights) == 0 {
		return nil, fmt.Errorf("emit: shape mix is empty")
	}
	m := &Mix{}
	// Iterate the set's order rather than the map's so the mix is deterministic
	// across processes, which Go's map iteration order would otherwise break.
	for _, name := range set.Names() {
		w, ok := weights[name]
		if !ok {
			continue
		}
		if w <= 0 {
			return nil, fmt.Errorf("emit: profile %q has non-positive weight %v", name, w)
		}
		p, _ := set.Get(name)
		m.total += w
		m.names = append(m.names, name)
		m.cumul = append(m.cumul, m.total)
		m.profile = append(m.profile, p)
	}
	if len(m.names) != len(weights) {
		for name := range weights {
			if _, ok := set.Get(name); !ok {
				return nil, fmt.Errorf("emit: shape mix names unknown profile %q", name)
			}
		}
	}
	if len(m.names) == 0 {
		return nil, fmt.Errorf("emit: shape mix selected no profiles")
	}
	return m, nil
}

// Pick selects a profile from the mix.
func (m *Mix) Pick(rnd *rand.Rand) Profile {
	if len(m.profile) == 1 {
		return m.profile[0]
	}
	r := rnd.Float64() * m.total
	for i, c := range m.cumul {
		if r < c {
			return m.profile[i]
		}
	}
	return m.profile[len(m.profile)-1]
}
