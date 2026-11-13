# The reference pipelines

This package is the project's conscience. It holds two toy collectors that copy
lines from a source directory to a sink directory.

**refgood** copies faithfully. The verifier must report it CLEAN.

**refbad** sabotages the copy. The verifier must catch it with exactly the
injected ranges.

The harness is only credible while both hold. Both run in CI on every commit,
and any change that flips either one blocks the merge.

## The sabotages, and the anomaly each must trigger

| Sabotage | What refbad does | Must surface as |
|---|---|---|
| `Drop` | Two ranges (37 and 11 sequences wide) are not copied at all | `MISSING`, with exactly those ranges |
| `Duplicate` | One range (9 wide) is written four times | `DUP_BUG`, with exactly that range |
| `TruncateContinuations` | For multi-line events in one range (60 wide), every continuation line after the first is withheld | `ORDER`, covering the affected parents |

Ranges are placed in disjoint quarters of the epoch, seeded from the run's
seed, and never touch the first or last sequence. Boundary bugs would otherwise
hide behind the sabotage.

## Why the expectation is derived, not written down

`Sabotage.Expect()` computes the required verdict from the sabotage struct
itself. A test that wrote its expectations separately could drift from what the
collector actually does. This cannot.

`compress()` is duplicated here rather than imported from `internal/verify` on
purpose. If the gate used the range compression it is testing, a bug in that
compression would cancel out on both sides and become invisible.

## The one place grading uses actuals

`TruncateContinuations` names a range of sequence numbers, but only multi-line
events have continuations to withhold, and the shape mix decides which those
are. The pipe therefore records which parents it really truncated, and the gate
grades against that.

Grading against intent would fail whenever a range happened to land on no stack
traces, which would make the gate flaky for a reason that has nothing to do
with the verifier.

## Running it

```sh
export LOSSLEDGER_HMAC_KEY="any-local-secret"
go run ./cmd/lossledger selfcheck -seqs 20000

# Keep the scratch directories to inspect what was written.
go run ./cmd/lossledger selfcheck -keep /tmp/gate
```

## If the gate fails

Do not weaken the expectation to make the run look clean. Prove the harness is
right with the reference pipeline, fix whichever side is actually wrong, and
record the reasoning in `docs/decisions/`.

A single false accusation against a real collector costs more credibility than
this project can afford.
