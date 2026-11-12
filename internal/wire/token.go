package wire

import (
	"errors"
	"strconv"
	"strings"
)

// Sentinels opening each of the three line kinds LossLedger writes.
const (
	SentinelCanary       = "LL1" // a canary line carrying a sequence number
	SentinelContinuation = "LLC" // a continuation line of a multi-line shape
	SentinelFinal        = "LLF" // the flush line closing an epoch cleanly
)

// hmacHexLen is the number of hex characters of HMAC-SHA256 kept in the token.
// 48 bits is far too short for a security boundary and is not meant to be one:
// it exists to keep foreign or corrupted lines out of the ledger, and it is
// sized so the token stays short enough to survive line-length limits.
const hmacHexLen = 12

// Parse errors. The verifier counts these rather than failing, because a
// pipeline under test is entitled to deliver us garbage.
var (
	ErrNoToken      = errors.New("wire: no token found in line")
	ErrMalformed    = errors.New("wire: malformed token")
	ErrBadSeq       = errors.New("wire: sequence number not a positive integer")
	ErrBadTimestamp = errors.New("wire: timestamp not an integer")
)

// Kind distinguishes the three line kinds.
type Kind uint8

const (
	KindCanary Kind = iota
	KindContinuation
	KindFinal
)

func (k Kind) String() string {
	switch k {
	case KindCanary:
		return SentinelCanary
	case KindContinuation:
		return SentinelContinuation
	case KindFinal:
		return SentinelFinal
	}
	return "unknown"
}

// Token is one parsed LossLedger marker.
//
// A canary token identifies exactly one emitted logical event. A continuation
// token carries the parent sequence number and its own index within the
// multi-line event. A final token closes an epoch and states the last sequence
// number the emitter wrote, which is what lets the verifier tell a clean
// shutdown from a crash.
type Token struct {
	Kind  Kind
	Src   string // emitting unit: node, pod or container
	Epoch string // ULID minted at emitter start
	Seq   uint64 // strictly monotonic within the epoch, starting at 1
	TS    int64  // emit time, unix nanoseconds
	Shape string // shape profile that produced the line
	HMAC  string // first 12 hex characters of the signature

	// ContIndex is the 1-based index of a continuation line within its parent
	// event. Meaningful only when Kind is KindContinuation.
	ContIndex int

	// ContTotal is how many continuation lines the parent event has.
	//
	// Without it a sink cannot tell a complete multi-line event from a
	// truncated one: it only ever sees the lines that arrived, so the highest
	// index present looks like the total. Carrying the expected count makes
	// partial delivery detectable from the sink side alone.
	ContTotal int

	// Final is the last sequence number of the epoch. Meaningful only when
	// Kind is KindFinal.
	Final uint64
}

// signingString is the exact byte sequence the HMAC covers. It deliberately
// excludes the shape so that re-shaping a profile does not invalidate a ledger
// recorded under the previous definition.
func (t Token) signingString() string {
	var b strings.Builder
	b.Grow(len(t.Src) + len(t.Epoch) + 48)
	b.WriteString(t.Src)
	b.WriteByte('|')
	b.WriteString(t.Epoch)
	b.WriteByte('|')
	b.WriteString(strconv.FormatUint(t.Seq, 10))
	b.WriteByte('|')
	b.WriteString(strconv.FormatInt(t.TS, 10))
	return b.String()
}

// Format renders the token and signs it with key.
func (t Token) Format(key []byte) string {
	t.HMAC = Sign(key, t.signingString())

	var b strings.Builder
	b.Grow(128)
	switch t.Kind {
	case KindContinuation:
		b.WriteString(SentinelContinuation)
		writeField(&b, "src", t.Src)
		writeField(&b, "ep", t.Epoch)
		writeField(&b, "seq", strconv.FormatUint(t.Seq, 10))
		writeField(&b, "n", strconv.Itoa(t.ContIndex))
		if t.ContTotal > 0 {
			// Omitted rather than written as zero when unknown: a reader that
			// sees no total falls back to the highest index, which is the old
			// behaviour and still parses.
			writeField(&b, "of", strconv.Itoa(t.ContTotal))
		}
		writeField(&b, "hmac", t.HMAC)
	case KindFinal:
		// A final line carries no sequence number of its own: the epoch's last
		// sequence is the "final" field, and writing seq=0 beside it would be
		// a second, contradictory answer to the same question.
		b.WriteString(SentinelFinal)
		writeField(&b, "src", t.Src)
		writeField(&b, "ep", t.Epoch)
		writeField(&b, "ts", strconv.FormatInt(t.TS, 10))
		writeField(&b, "final", strconv.FormatUint(t.Final, 10))
		writeField(&b, "hmac", t.HMAC)
	default:
		b.WriteString(SentinelCanary)
		writeField(&b, "src", t.Src)
		writeField(&b, "ep", t.Epoch)
		writeField(&b, "seq", strconv.FormatUint(t.Seq, 10))
		writeField(&b, "ts", strconv.FormatInt(t.TS, 10))
		writeField(&b, "shape", t.Shape)
		writeField(&b, "hmac", t.HMAC)
	}
	return b.String()
}

func writeField(b *strings.Builder, key, value string) {
	b.WriteByte('|')
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(value)
}

// Verify reports whether the token's HMAC matches a signature computed with
// key. A token that fails this never enters the ledger.
func (t Token) Verify(key []byte) bool {
	return Equal(t.HMAC, Sign(key, t.signingString()))
}

// Extract finds the first LossLedger token anywhere in line and parses it.
//
// Shape profiles are free to wrap the token: inside a JSON field, after a
// timestamp prefix a collector added, in the middle of a stack trace. Extract
// locates the sentinel and reads forward, so any wrapping that leaves the token
// contiguous survives. A pipeline that splits the token across physical lines
// has mangled the record, and we would rather report that as an invalid line
// than guess at reassembly.
func Extract(line string) (Token, error) {
	idx, kind := findSentinel(line)
	if idx < 0 {
		return Token{}, ErrNoToken
	}
	return parseFrom(line[idx:], kind)
}

// findSentinel returns the offset of the earliest sentinel in line, and which
// kind it was. It requires the sentinel be followed by '|' so that prose
// mentioning LL1 does not register as a token.
func findSentinel(line string) (int, Kind) {
	best := -1
	var bestKind Kind
	for _, c := range []struct {
		sentinel string
		kind     Kind
	}{
		{SentinelCanary, KindCanary},
		{SentinelContinuation, KindContinuation},
		{SentinelFinal, KindFinal},
	} {
		from := 0
		for {
			i := strings.Index(line[from:], c.sentinel+"|")
			if i < 0 {
				break
			}
			at := from + i
			if best < 0 || at < best {
				best, bestKind = at, c.kind
			}
			break
		}
	}
	return best, bestKind
}

// parseFrom reads key=value fields separated by '|' until the hmac field, which
// is terminal by construction. Unknown fields are skipped rather than rejected
// so that a future LL1 revision adding fields stays readable by this parser.
func parseFrom(s string, kind Kind) (Token, error) {
	t := Token{Kind: kind}

	rest := s[len(kind.String()):]
	if !strings.HasPrefix(rest, "|") {
		return Token{}, ErrMalformed
	}
	rest = rest[1:]

	var sawHMAC bool
	for len(rest) > 0 && !sawHMAC {
		var field string
		if i := strings.IndexByte(rest, '|'); i >= 0 {
			field, rest = rest[:i], rest[i+1:]
		} else {
			field, rest = rest, ""
		}

		eq := strings.IndexByte(field, '=')
		if eq < 0 {
			// A field without '=' means the token ended and we have run into
			// whatever the shape profile wrapped around it.
			break
		}
		key, value := field[:eq], field[eq+1:]

		switch key {
		case "src":
			t.Src = value
		case "ep":
			t.Epoch = value
		case "seq":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return Token{}, ErrBadSeq
			}
			// Sequences start at 1, so zero is only legal on a final line,
			// which carries the field for forward compatibility but does not
			// use it.
			if n == 0 && kind != KindFinal {
				return Token{}, ErrBadSeq
			}
			t.Seq = n
		case "ts":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return Token{}, ErrBadTimestamp
			}
			t.TS = n
		case "shape":
			t.Shape = value
		case "n":
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return Token{}, ErrMalformed
			}
			t.ContIndex = n
		case "of":
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return Token{}, ErrMalformed
			}
			t.ContTotal = n
		case "final":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return Token{}, ErrMalformed
			}
			t.Final = n
		case "hmac":
			// The signature is fixed width, so anything the wrapping appended
			// past it (a closing quote, a brace) is not ours.
			if len(value) < hmacHexLen {
				return Token{}, ErrMalformed
			}
			t.HMAC = value[:hmacHexLen]
			sawHMAC = true
		default:
			// Forward compatibility: ignore fields this version does not know.
		}
	}

	if t.Src == "" || t.Epoch == "" || t.HMAC == "" {
		return Token{}, ErrMalformed
	}
	if t.Kind != KindFinal && t.Seq == 0 {
		return Token{}, ErrMalformed
	}
	if !ValidULID(t.Epoch) {
		return Token{}, ErrMalformed
	}
	return t, nil
}
