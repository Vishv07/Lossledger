package verify

import (
	"fmt"
	"strings"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/Vishv07/lossledger/internal/adapter"
)

// Ledger is the set of sequence numbers in some state for one
// (source, epoch, sink): arrived, settled, already re-checked.
//
// A roaring bitmap is the right shape here because the sets are dense runs with
// occasional holes, which is exactly what roaring compresses well. A day of
// 200 sources at 10 lines per second is about 173 million sequences, and
// storing those as a hash set would not fit in the verifier's memory budget
// (PRD section 8.2).
type Ledger struct {
	mu sync.RWMutex
	bm *roaring.Bitmap
}

// NewLedger returns an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{bm: roaring.New()}
}

// LedgerOf returns a ledger containing exactly the given sequences. Tests and
// the reference pipeline use it to state an expectation directly.
func LedgerOf(seqs ...uint64) *Ledger {
	l := NewLedger()
	for _, s := range seqs {
		l.bm.Add(s)
	}
	return l
}

// LedgerRange returns a ledger containing every sequence in [from, to].
func LedgerRange(from, to uint64) *Ledger {
	l := NewLedger()
	if to >= from {
		l.bm.AddRange(from, to+1)
	}
	return l
}

// Add records one sequence.
func (l *Ledger) Add(seq uint64) {
	l.mu.Lock()
	l.bm.Add(seq)
	l.mu.Unlock()
}

// AddRange records every sequence in [from, to].
func (l *Ledger) AddRange(from, to uint64) {
	if to < from {
		return
	}
	l.mu.Lock()
	l.bm.AddRange(from, to+1)
	l.mu.Unlock()
}

// Remove drops one sequence.
func (l *Ledger) Remove(seq uint64) {
	l.mu.Lock()
	l.bm.Remove(seq)
	l.mu.Unlock()
}

// Contains reports membership.
func (l *Ledger) Contains(seq uint64) bool {
	if l == nil {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.bm.Contains(seq)
}

// Count returns the cardinality.
func (l *Ledger) Count() uint64 {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.bm.GetCardinality()
}

// CountInRange returns how many members fall in [from, to].
func (l *Ledger) CountInRange(from, to uint64) uint64 {
	if l == nil || to < from {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	sub := roaring.New()
	sub.AddRange(from, to+1)
	sub.And(l.bm)
	return sub.GetCardinality()
}

// Max returns the highest member, or zero when empty.
func (l *Ledger) Max() uint64 {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.bm.IsEmpty() {
		return 0
	}
	return l.bm.Maximum()
}

// Slice materialises the members in ascending order. Intended for ranges that
// have already been narrowed, not for a whole epoch.
func (l *Ledger) Slice() []uint64 {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.bm.ToArray()
}

// MissingIn returns the sequences in [from, to] that the ledger does not hold.
func (l *Ledger) MissingIn(from, to uint64) []uint64 {
	if to < from {
		return nil
	}
	want := roaring.New()
	want.AddRange(from, to+1)

	if l != nil {
		l.mu.RLock()
		want.AndNot(l.bm)
		l.mu.RUnlock()
	}
	return want.ToArray()
}

// Clone returns an independent copy.
func (l *Ledger) Clone() *Ledger {
	if l == nil {
		return NewLedger()
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return &Ledger{bm: l.bm.Clone()}
}

// Union adds every member of other.
func (l *Ledger) Union(other *Ledger) {
	if other == nil {
		return
	}
	other.mu.RLock()
	clone := other.bm.Clone()
	other.mu.RUnlock()

	l.mu.Lock()
	l.bm.Or(clone)
	l.mu.Unlock()
}

// Equal reports whether two ledgers hold the same sequences.
func (l *Ledger) Equal(other *Ledger) bool {
	if l == nil || other == nil {
		return l.Count() == 0 && other.Count() == 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()
	return l.bm.Equals(other.bm)
}

// Ranges returns the ledger's members as compressed inclusive ranges.
func (l *Ledger) Ranges() []adapter.Range {
	return CompressRanges(l.Slice())
}

// Bytes returns the serialised size, which the verifier exports so an operator
// can see its own memory cost.
func (l *Ledger) Bytes() uint64 {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.bm.GetSerializedSizeInBytes()
}

// MarshalBinary serialises the ledger for the embedded state store, so a
// verifier restart resumes from what it knew rather than re-deriving it and
// briefly reporting phantom gaps.
func (l *Ledger) MarshalBinary() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.bm.MarshalBinary()
}

// UnmarshalBinary restores a serialised ledger.
func (l *Ledger) UnmarshalBinary(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bm == nil {
		l.bm = roaring.New()
	}
	return l.bm.UnmarshalBinary(data)
}

// CompressRanges turns a sorted sequence list into inclusive ranges.
//
// Reports quote ranges rather than individual numbers because "2041 to 2118"
// is a fact a maintainer can act on, while a column of 78 integers is not.
func CompressRanges(seqs []uint64) []adapter.Range {
	if len(seqs) == 0 {
		return nil
	}

	out := make([]adapter.Range, 0, 8)
	start, prev := seqs[0], seqs[0]

	for _, seq := range seqs[1:] {
		if seq == prev {
			continue // tolerate duplicates in the input
		}
		if seq == prev+1 {
			prev = seq
			continue
		}
		out = append(out, adapter.Range{From: start, To: prev})
		start, prev = seq, seq
	}
	out = append(out, adapter.Range{From: start, To: prev})
	return out
}

// FormatRanges renders ranges compactly, truncating long lists so a report
// stays readable when a whole epoch went missing.
func FormatRanges(ranges []adapter.Range) string {
	const maxShown = 12

	if len(ranges) == 0 {
		return ""
	}
	parts := make([]string, 0, maxShown+1)
	for i, r := range ranges {
		if i == maxShown {
			parts = append(parts, fmt.Sprintf("and %d more", len(ranges)-maxShown))
			break
		}
		parts = append(parts, r.String())
	}
	return strings.Join(parts, ", ")
}

// TotalWidth sums the widths of every range.
func TotalWidth(ranges []adapter.Range) uint64 {
	var total uint64
	for _, r := range ranges {
		total += r.Width()
	}
	return total
}
