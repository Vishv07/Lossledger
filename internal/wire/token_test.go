package wire

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var testKey = []byte("lossledger-test-key")

func sampleToken() Token {
	return Token{
		Kind:  KindCanary,
		Src:   "node-a",
		Epoch: MustULID(time.UnixMilli(1_700_000_000_000)).String(),
		Seq:   4096,
		TS:    1_700_000_123_456_789,
		Shape: "plain",
	}
}

func TestTokenRoundTrip(t *testing.T) {
	original := sampleToken()
	line := original.Format(testKey)

	parsed, err := Extract(line)
	if err != nil {
		t.Fatalf("Extract(%q): %v", line, err)
	}
	if parsed.Src != original.Src {
		t.Errorf("Src = %q, want %q", parsed.Src, original.Src)
	}
	if parsed.Epoch != original.Epoch {
		t.Errorf("Epoch = %q, want %q", parsed.Epoch, original.Epoch)
	}
	if parsed.Seq != original.Seq {
		t.Errorf("Seq = %d, want %d", parsed.Seq, original.Seq)
	}
	if parsed.TS != original.TS {
		t.Errorf("TS = %d, want %d", parsed.TS, original.TS)
	}
	if parsed.Shape != original.Shape {
		t.Errorf("Shape = %q, want %q", parsed.Shape, original.Shape)
	}
	if !parsed.Verify(testKey) {
		t.Error("round-tripped token failed its own signature check")
	}
}

// TestTokenSurvivesWrapping is the property that matters most in practice: a
// collector or a shape profile may put arbitrary text around the token, and we
// must still find it. Each case embeds the same token differently.
func TestTokenSurvivesWrapping(t *testing.T) {
	tok := sampleToken()
	raw := tok.Format(testKey)

	cases := []struct {
		name string
		line string
	}{
		{"bare", raw},
		{"json field", fmt.Sprintf(`{"level":"info","msg":%q,"host":"a"}`, raw)},
		{"syslog prefix", "Nov 11 12:00:00 host app[123]: " + raw},
		{"stack trace body", "java.lang.NullPointerException: " + raw + "\n\tat com.example.Main"},
		{"leading whitespace", "        " + raw},
		{"trailing noise", raw + " <-- emitted by canary"},
		{"docker json driver", fmt.Sprintf(`{"log":%q,"stream":"stdout","time":"2026-11-11T12:00:00Z"}`, raw+"\n")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Extract(tc.line)
			if err != nil {
				t.Fatalf("Extract: %v", err)
			}
			if parsed.Seq != tok.Seq {
				t.Errorf("Seq = %d, want %d", parsed.Seq, tok.Seq)
			}
			if !parsed.Verify(testKey) {
				t.Errorf("signature failed after wrapping; hmac parsed as %q", parsed.HMAC)
			}
		})
	}
}

func TestTokenForwardCompatibility(t *testing.T) {
	// A future LL1 revision may add fields before the terminal hmac. This
	// parser must read what it knows and ignore the rest, because an old
	// verifier reading new lines is a normal state during a rollout.
	tok := sampleToken()
	signed := Sign(testKey, tok.signingString())
	line := fmt.Sprintf("LL1|src=%s|ep=%s|seq=%d|ts=%d|shape=%s|region=eu-west-1|tier=2|hmac=%s",
		tok.Src, tok.Epoch, tok.Seq, tok.TS, tok.Shape, signed)

	parsed, err := Extract(line)
	if err != nil {
		t.Fatalf("Extract rejected a line with unknown fields: %v", err)
	}
	if parsed.Seq != tok.Seq {
		t.Errorf("Seq = %d, want %d", parsed.Seq, tok.Seq)
	}
	if !parsed.Verify(testKey) {
		t.Error("signature failed on a line carrying unknown fields")
	}
}

func TestTokenRejectsTampering(t *testing.T) {
	tok := sampleToken()
	line := tok.Format(testKey)

	// Rewriting the sequence number must invalidate the signature. This is the
	// property that keeps a replayed or hand-edited line out of the ledger.
	tampered := strings.Replace(line, "seq=4096", "seq=4097", 1)
	parsed, err := Extract(tampered)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if parsed.Verify(testKey) {
		t.Error("a token with a rewritten sequence number passed verification")
	}
}

func TestTokenRejectsForeignKey(t *testing.T) {
	line := sampleToken().Format(testKey)
	parsed, err := Extract(line)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Verify([]byte("a-different-deployment-key")) {
		t.Error("a token signed by another deployment passed verification")
	}
}

func TestExtractErrors(t *testing.T) {
	validEpoch := MustULID(time.Now()).String()

	cases := []struct {
		name string
		line string
		want error
	}{
		{"empty line", "", ErrNoToken},
		{"ordinary log line", "2026-11-11 INFO starting server on :8080", ErrNoToken},
		{"sentinel without pipe", "LL1 is our format version", ErrNoToken},
		{"prose mentioning the token", "see the LL1 spec for details", ErrNoToken},
		{
			"zero sequence",
			fmt.Sprintf("LL1|src=a|ep=%s|seq=0|ts=1|shape=plain|hmac=0123456789ab", validEpoch),
			ErrBadSeq,
		},
		{
			"non-numeric sequence",
			fmt.Sprintf("LL1|src=a|ep=%s|seq=abc|ts=1|shape=plain|hmac=0123456789ab", validEpoch),
			ErrBadSeq,
		},
		{
			"non-numeric timestamp",
			fmt.Sprintf("LL1|src=a|ep=%s|seq=1|ts=xyz|shape=plain|hmac=0123456789ab", validEpoch),
			ErrBadTimestamp,
		},
		{
			"missing hmac",
			fmt.Sprintf("LL1|src=a|ep=%s|seq=1|ts=1|shape=plain", validEpoch),
			ErrMalformed,
		},
		{
			"truncated hmac",
			fmt.Sprintf("LL1|src=a|ep=%s|seq=1|ts=1|shape=plain|hmac=0123", validEpoch),
			ErrMalformed,
		},
		{
			"epoch is not a ULID",
			"LL1|src=a|ep=not-a-ulid|seq=1|ts=1|shape=plain|hmac=0123456789ab",
			ErrMalformed,
		},
		{
			"missing src",
			fmt.Sprintf("LL1|ep=%s|seq=1|ts=1|shape=plain|hmac=0123456789ab", validEpoch),
			ErrMalformed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Extract(tc.line)
			if err != tc.want {
				t.Errorf("Extract(%q) error = %v, want %v", tc.line, err, tc.want)
			}
		})
	}
}

func TestContinuationToken(t *testing.T) {
	tok := Token{
		Kind:      KindContinuation,
		Src:       "node-a",
		Epoch:     MustULID(time.Now()).String(),
		Seq:       77,
		ContIndex: 3,
		ContTotal: 12,
	}
	line := tok.Format(testKey)
	if !strings.HasPrefix(line, SentinelContinuation+"|") {
		t.Fatalf("continuation line has wrong sentinel: %q", line)
	}

	parsed, err := Extract(line)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if parsed.Kind != KindContinuation {
		t.Errorf("Kind = %v, want continuation", parsed.Kind)
	}
	if parsed.Seq != 77 {
		t.Errorf("Seq = %d, want 77", parsed.Seq)
	}
	if parsed.ContIndex != 3 {
		t.Errorf("ContIndex = %d, want 3", parsed.ContIndex)
	}
	if parsed.ContTotal != 12 {
		t.Errorf("ContTotal = %d, want 12: without the declared total a sink cannot tell a truncated event from a whole one", parsed.ContTotal)
	}
	if !parsed.Verify(testKey) {
		t.Error("continuation token failed verification")
	}
}

func TestFinalToken(t *testing.T) {
	// The final line is what distinguishes a clean shutdown from a crash. An
	// epoch closed by one of these has a known last sequence number, so the
	// verifier may legitimately declare anything below it missing.
	tok := Token{
		Kind:  KindFinal,
		Src:   "node-a",
		Epoch: MustULID(time.Now()).String(),
		Seq:   0,
		TS:    time.Now().UnixNano(),
		Final: 50_000,
	}
	line := tok.Format(testKey)

	parsed, err := Extract(line)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if parsed.Kind != KindFinal {
		t.Errorf("Kind = %v, want final", parsed.Kind)
	}
	if parsed.Final != 50_000 {
		t.Errorf("Final = %d, want 50000", parsed.Final)
	}
	if !parsed.Verify(testKey) {
		t.Error("final token failed verification")
	}
}

func TestExtractFindsEarliestSentinel(t *testing.T) {
	// When a line somehow carries two tokens, we take the first and let the
	// second be someone else's problem. Deterministic beats clever.
	a := Token{Kind: KindCanary, Src: "a", Epoch: MustULID(time.Now()).String(), Seq: 1, TS: 1, Shape: "plain"}
	b := Token{Kind: KindCanary, Src: "b", Epoch: MustULID(time.Now()).String(), Seq: 2, TS: 2, Shape: "plain"}

	parsed, err := Extract(a.Format(testKey) + " " + b.Format(testKey))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Src != "a" {
		t.Errorf("Src = %q, want the first token on the line", parsed.Src)
	}
}

func BenchmarkExtract(b *testing.B) {
	line := fmt.Sprintf(`{"ts":"2026-11-11T12:00:00Z","msg":%q}`, sampleToken().Format(testKey))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Extract(line); err != nil {
			b.Fatal(err)
		}
	}
}
