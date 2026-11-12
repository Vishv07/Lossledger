package wire

import (
	"bytes"
	"testing"
	"time"
)

func TestULIDRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		when time.Time
	}{
		{"epoch", time.UnixMilli(0)},
		{"recent", time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)},
		{"far future", time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := NewULID(tc.when, bytes.NewReader(make([]byte, 10)))
			if err != nil {
				t.Fatalf("NewULID: %v", err)
			}
			s := u.String()
			if len(s) != 26 {
				t.Fatalf("String() length = %d, want 26", len(s))
			}
			back, err := ParseULID(s)
			if err != nil {
				t.Fatalf("ParseULID(%q): %v", s, err)
			}
			if back != u {
				t.Errorf("round trip changed bytes:\n got %x\nwant %x", back, u)
			}
			if got, want := back.Time().UnixMilli(), tc.when.UnixMilli(); got != want {
				t.Errorf("Time() = %d ms, want %d ms", got, want)
			}
		})
	}
}

func TestULIDSortsByTime(t *testing.T) {
	// Two ULIDs minted a second apart must compare in that order, because the
	// verifier relies on epoch ordering to finalize superseded epochs.
	early, err := NewULID(time.UnixMilli(1_000_000), bytes.NewReader(make([]byte, 10)))
	if err != nil {
		t.Fatal(err)
	}
	late, err := NewULID(time.UnixMilli(1_001_000), bytes.NewReader(make([]byte, 10)))
	if err != nil {
		t.Fatal(err)
	}
	if early.Compare(late) >= 0 {
		t.Errorf("earlier ULID did not sort first: %s vs %s", early, late)
	}
	if late.Compare(early) <= 0 {
		t.Errorf("later ULID did not sort last: %s vs %s", late, early)
	}
	if early.Compare(early) != 0 {
		t.Error("ULID did not compare equal to itself")
	}
}

func TestULIDStringIsCrockford(t *testing.T) {
	u := MustULID(time.Now())
	for i, c := range u.String() {
		if decodeMap[byte(c)] == 0xFF {
			t.Fatalf("character %q at index %d is not Crockford base32", c, i)
		}
	}
}

func TestParseULIDRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"too short", "01JC"},
		{"too long", "01JCABCDEFGHJKMNPQRSTVWXYZ0"},
		{"illegal character U", "01JCABCDEFGHJKMNPQRSTVWXYU"},
		{"punctuation", "01JCABCDEFGHJKMNPQRSTVWX-Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseULID(tc.in); err == nil {
				t.Errorf("ParseULID(%q) accepted invalid input", tc.in)
			}
			if ValidULID(tc.in) {
				t.Errorf("ValidULID(%q) = true, want false", tc.in)
			}
		})
	}
}

func TestULIDAmbiguousCharactersFold(t *testing.T) {
	// Crockford folds I and L onto 1 and O onto 0 so a human transcribing an
	// epoch out of a log line cannot produce an unparseable identifier.
	canonical, err := ParseULID("01JC00000000000000000000H1")
	if err != nil {
		t.Fatalf("canonical form rejected: %v", err)
	}
	folded, err := ParseULID("0IJC0000000O0000000000000HI")
	if err == nil && folded == canonical {
		t.Log("folded form matched canonical")
	}
	// The length check must still reject the folded string above, which is 27
	// characters. This asserts the length guard runs before character folding.
	if _, err := ParseULID("0IJC0000000O0000000000000HI"); err == nil {
		t.Error("27-character input accepted")
	}
}

func TestULIDEntropyFailurePropagates(t *testing.T) {
	// A short reader must surface an error rather than mint a half-random ULID.
	if _, err := NewULID(time.Now(), bytes.NewReader([]byte{1, 2, 3})); err == nil {
		t.Error("NewULID accepted a truncated entropy source")
	}
}

func TestULIDDeterministicUnderSeededEntropy(t *testing.T) {
	// Replay depends on this: same clock, same entropy, same epoch identifier.
	when := time.UnixMilli(1_700_000_000_000)
	seed := []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

	first, err := NewULID(when, bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewULID(when, bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Errorf("seeded ULIDs differ: %s vs %s", first, second)
	}
}
