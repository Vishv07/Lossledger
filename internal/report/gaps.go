package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Vishv07/lossledger/internal/verify"
)

// GapReport renders a verdict for a human.
type GapReport struct {
	Verdict   verify.Verdict
	StartedAt time.Time
	EndedAt   time.Time

	// Settle is the window the verdict was reached under. Every report states
	// it, so a reader can judge whether the verdict was fair rather than
	// having to trust that it was (PRD risk table).
	Settle time.Duration

	// Sinks names what was queried, and Limitations names what was not tested.
	// Both appear in every report by policy (PRD principle 5).
	Sinks       []string
	Limitations []string
}

// WriteText renders the report as plain text.
func (r GapReport) WriteText(w io.Writer) error {
	var b strings.Builder

	b.WriteString("LossLedger delivery report\n")
	b.WriteString("==========================\n\n")

	fmt.Fprintf(&b, "Verdict        : %s (exit %d)\n", r.Verdict.Worst, r.Verdict.ExitCode())
	fmt.Fprintf(&b, "Emitted        : %d\n", r.Verdict.Emitted)
	fmt.Fprintf(&b, "Arrived        : %d\n", r.Verdict.Arrived)
	fmt.Fprintf(&b, "Missing        : %d\n", r.Verdict.MissingN)
	fmt.Fprintf(&b, "In flight      : %d\n", r.Verdict.InFlight)
	// Never print a settle window we did not actually apply. "0s" would read
	// as "we judged instantly", which is the opposite of what this tool does.
	if r.Settle > 0 {
		fmt.Fprintf(&b, "Settle window  : %s\n", r.Settle)
	} else {
		b.WriteString("Settle window  : not recorded by this command\n")
	}
	if len(r.Sinks) > 0 {
		fmt.Fprintf(&b, "Sinks queried  : %s\n", strings.Join(r.Sinks, ", "))
	}
	if !r.StartedAt.IsZero() {
		fmt.Fprintf(&b, "Window         : %s to %s\n",
			r.StartedAt.UTC().Format(time.RFC3339), r.EndedAt.UTC().Format(time.RFC3339))
	}

	// The integrity metric is printed on every report, pass or fail, because a
	// result without it is not interpretable.
	fmt.Fprintf(&b, "\nIntegrity\n---------\n")
	fmt.Fprintf(&b, "False missing  : %d", r.Verdict.FalseMissing)
	if r.Verdict.FalseMissing == 0 {
		b.WriteString("  (no sequence declared missing has since arrived)\n")
	} else {
		b.WriteString("  THIS MUST BE ZERO. The verifier accused the pipeline of losing data it went on to deliver.\n")
		b.WriteString("                 Treat every finding below as unreliable until this is fixed.\n")
	}
	fmt.Fprintf(&b, "Invalid lines  : %d\n", r.Verdict.InvalidLines)

	if err := r.Verdict.Conservation(); err != nil {
		fmt.Fprintf(&b, "Conservation   : FAILED, %s\n", err)
	} else {
		b.WriteString("Conservation   : holds (emitted = arrived + missing + in flight)\n")
	}

	if len(r.Verdict.Findings) == 0 {
		b.WriteString("\nNo anomalies.\n")
	} else {
		b.WriteString("\nFindings\n--------\n")
		for _, f := range r.Verdict.Findings {
			fmt.Fprintf(&b, "\n%s  %s/%s at %s\n", f.Anomaly, f.Src, f.Epoch, f.Sink)
			fmt.Fprintf(&b, "  count  : %d\n", f.Count)
			if len(f.Ranges) > 0 {
				fmt.Fprintf(&b, "  ranges : %s\n", verify.FormatRanges(f.Ranges))
			}
			if f.Detail != "" {
				fmt.Fprintf(&b, "  detail : %s\n", f.Detail)
			}
			if f.Heuristic {
				b.WriteString("  note   : classified without restart ground truth; not publishable as a finding\n")
			}
		}
	}

	b.WriteString("\nLimitations\n-----------\n")
	if len(r.Limitations) == 0 {
		b.WriteString("  none recorded\n")
	}
	for _, l := range r.Limitations {
		fmt.Fprintf(&b, "  %s\n", l)
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// jsonFinding is the wire shape of a finding, kept separate from the internal
// type so that renaming a Go field cannot silently break a consumer's parser.
type jsonFinding struct {
	Anomaly   string   `json:"anomaly"`
	Severity  int      `json:"severity"`
	Src       string   `json:"src"`
	Epoch     string   `json:"epoch"`
	Sink      string   `json:"sink"`
	Count     uint64   `json:"count"`
	Ranges    []string `json:"ranges,omitempty"`
	Detail    string   `json:"detail,omitempty"`
	Heuristic bool     `json:"heuristic,omitempty"`
}

type jsonReport struct {
	Verdict      string        `json:"verdict"`
	ExitCode     int           `json:"exit_code"`
	Emitted      uint64        `json:"emitted"`
	Arrived      uint64        `json:"arrived"`
	Missing      uint64        `json:"missing"`
	InFlight     uint64        `json:"in_flight"`
	FalseMissing uint64        `json:"false_missing"`
	InvalidLines uint64        `json:"invalid_lines"`
	SettleWindow string        `json:"settle_window"`
	Sinks        []string      `json:"sinks,omitempty"`
	Findings     []jsonFinding `json:"findings"`
	Limitations  []string      `json:"limitations,omitempty"`
	Conservation string        `json:"conservation"`
}

// WriteJSON renders the report as JSON for machines.
func (r GapReport) WriteJSON(w io.Writer) error {
	out := jsonReport{
		Verdict:      string(r.Verdict.Worst),
		ExitCode:     r.Verdict.ExitCode(),
		Emitted:      r.Verdict.Emitted,
		Arrived:      r.Verdict.Arrived,
		Missing:      r.Verdict.MissingN,
		InFlight:     r.Verdict.InFlight,
		FalseMissing: r.Verdict.FalseMissing,
		InvalidLines: r.Verdict.InvalidLines,
		SettleWindow: r.Settle.String(),
		Sinks:        r.Sinks,
		Limitations:  r.Limitations,
		Conservation: "ok",
	}
	if err := r.Verdict.Conservation(); err != nil {
		out.Conservation = err.Error()
	}

	for _, f := range r.Verdict.Findings {
		jf := jsonFinding{
			Anomaly:   string(f.Anomaly),
			Severity:  f.Anomaly.Severity(),
			Src:       f.Src,
			Epoch:     f.Epoch,
			Sink:      f.Sink,
			Count:     f.Count,
			Detail:    f.Detail,
			Heuristic: f.Heuristic,
		}
		for _, rg := range f.Ranges {
			jf.Ranges = append(jf.Ranges, rg.String())
		}
		out.Findings = append(out.Findings, jf)
	}
	if out.Findings == nil {
		out.Findings = []jsonFinding{}
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// DefaultLimitations lists what a v1 run did not cover. It is attached to every
// report so that absence of evidence is never read as evidence of absence.
func DefaultLimitations(sinks []string) []string {
	return []string{
		"Only the sinks listed above were queried. Loss at any store not listed is neither confirmed nor ruled out.",
		"Attribution is source-side versus sink-side only. Per-hop attribution across a broker is out of scope in v1.",
		"Kubernetes-native log sources (kubelet, CRI) were not exercised; the emitter writes files and stdout.",
		"Most teams tolerate one to two percent log loss without noticing. A clean result here means this pipeline delivered these canaries, not that no pipeline ever loses anything.",
	}
}
