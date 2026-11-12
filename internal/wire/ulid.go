package wire

import (
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"time"
)

// ULID is a 128-bit lexicographically sortable identifier: a 48-bit big-endian
// millisecond timestamp followed by 80 bits of entropy, rendered as 26
// characters of Crockford base32.
//
// We implement the subset we need rather than taking a dependency. LossLedger
// only ever mints and compares epoch identifiers; it never parses ULIDs minted
// elsewhere, so the full specification (monotonic entropy, strict decoding of
// every ambiguous character) is more surface than the project requires.
type ULID [16]byte

// crockford is Crockford base32: the digits plus the uppercase alphabet with
// I, L, O and U removed so that no character is mistakable for another.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ErrInvalidULID reports a string that is not 26 characters of Crockford base32.
var ErrInvalidULID = errors.New("wire: invalid ULID")

// decodeMap maps a byte to its Crockford value, or 0xFF when the byte is not a
// legal character. Ambiguous characters fold onto their canonical digit the way
// Crockford specifies: I and L read as 1, O reads as 0.
var decodeMap [256]byte

func init() {
	for i := range decodeMap {
		decodeMap[i] = 0xFF
	}
	for i := 0; i < len(crockford); i++ {
		decodeMap[crockford[i]] = byte(i)
		decodeMap[lower(crockford[i])] = byte(i)
	}
	for _, p := range []struct {
		char  byte
		value byte
	}{{'I', 1}, {'L', 1}, {'O', 0}} {
		decodeMap[p.char] = p.value
		decodeMap[lower(p.char)] = p.value
	}
}

func lower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

// NewULID mints a ULID stamped with t and filled from entropy. Passing a nil
// entropy source uses crypto/rand. The gauntlet passes a seeded source so that
// a replayed run mints the same epoch identifiers as the run it replays.
func NewULID(t time.Time, entropy io.Reader) (ULID, error) {
	var u ULID

	ms := uint64(t.UnixMilli())
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)

	if entropy == nil {
		entropy = rand.Reader
	}
	if _, err := io.ReadFull(entropy, u[6:]); err != nil {
		return ULID{}, err
	}
	return u, nil
}

// MustULID is NewULID with crypto/rand, panicking only if the system entropy
// source fails. Reserved for process startup, never for request paths.
func MustULID(t time.Time) ULID {
	u, err := NewULID(t, nil)
	if err != nil {
		panic("wire: system entropy unavailable: " + err.Error())
	}
	return u
}

// String renders the ULID as 26 Crockford base32 characters. The 128 bits are
// written into 130 bits of output, so the leading character carries only the
// top 2 bits and a ULID never begins above '7'.
func (u ULID) String() string {
	var out [26]byte
	var acc uint16
	var bits uint
	pos := 26

	for i := 15; i >= 0; i-- {
		acc |= uint16(u[i]) << bits
		bits += 8
		for bits >= 5 {
			pos--
			out[pos] = crockford[acc&0x1F]
			acc >>= 5
			bits -= 5
		}
	}
	if pos > 0 {
		out[pos-1] = crockford[acc&0x1F]
		pos--
	}
	for pos > 0 {
		pos--
		out[pos] = '0'
	}
	return string(out[:])
}

// Time returns the millisecond timestamp encoded in the ULID's first 48 bits.
func (u ULID) Time() time.Time {
	ms := uint64(u[0])<<40 | uint64(u[1])<<32 | uint64(u[2])<<24 |
		uint64(u[3])<<16 | uint64(u[4])<<8 | uint64(u[5])
	return time.UnixMilli(int64(ms)).UTC()
}

// ParseULID decodes 26 characters of Crockford base32.
func ParseULID(s string) (ULID, error) {
	if len(s) != 26 {
		return ULID{}, ErrInvalidULID
	}
	var acc uint16
	var bits uint
	var u ULID
	pos := 16

	for i := len(s) - 1; i >= 0; i-- {
		v := decodeMap[s[i]]
		if v == 0xFF {
			return ULID{}, ErrInvalidULID
		}
		acc |= uint16(v) << bits
		bits += 5
		for bits >= 8 {
			pos--
			if pos < 0 {
				// More significant bits than a ULID holds.
				if acc&0xFF != 0 {
					return ULID{}, ErrInvalidULID
				}
				break
			}
			u[pos] = byte(acc)
			acc >>= 8
			bits -= 8
		}
	}
	return u, nil
}

// ValidULID reports whether s is a well-formed ULID string. The verifier calls
// this on every parsed line, so it avoids allocating.
func ValidULID(s string) bool {
	if len(s) != 26 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if decodeMap[s[i]] == 0xFF {
			return false
		}
	}
	return true
}

// Compare orders two ULIDs bytewise, which for ULIDs is also time order.
func (u ULID) Compare(other ULID) int {
	return strings.Compare(string(u[:]), string(other[:]))
}
