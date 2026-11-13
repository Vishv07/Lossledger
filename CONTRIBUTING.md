# Contributing

Thanks for looking. LossLedger accuses infrastructure of losing data, so the
bar for changes is set by one question: could this change make us accuse a
pipeline wrongly?

## Development setup

```sh
go build ./...
go test ./...

export LOSSLEDGER_HMAC_KEY="any-local-secret"
go run ./cmd/lossledger selfcheck
```

## The one rule

**The self-validation gate must pass.** `internal/refpipe` holds two reference
collectors: refgood copies faithfully, refbad sabotages the copy in precisely
described ways. The gate requires the verifier to report refgood clean and to
catch refbad with exactly the injected ranges.

Any change that flips either one blocks the merge. If you believe the gate is
wrong, change the gate in its own commit with the reasoning, not as a side
effect of a feature.

## Adding a sink adapter

Implement `adapter.Sink`. Aim for 150 to 250 lines; much more usually means
store-specific knowledge has leaked in that belongs in the docs.

- `CountRange` is the cheap path and is called over wide ranges. It must be
  cheap, because the verifier's whole cost model depends on it.
- `FetchRange` is called only on ranges that already disagree.
- `SettleHint` must be honest. Reporting a shorter consistency delay than your
  store really has will make us accuse it of loss while it is still writing.
- `Doctor` must verify the canary fields are queryable, not just that the
  connection works. A sink that answers queries but cannot see the token fields
  would report total loss.
- Implement `adapter.PartialReporter` if your store can answer which sequences
  arrived with incomplete multi-line bodies. If it cannot, do not implement it,
  so the report can say partial detection was unavailable rather than claim
  none were found.

## Adding a shape profile

Profiles are data. Add a file to `profiles/` and a case in `emit.Builtins()`.
Loss is content-dependent: a 64 KB line and a 40-line stack trace fail in
different places than a tidy one-liner, which is why the set exists.

## Pull requests

The template asks one question: **does this change what gets classified as
MISSING or DUP? Yes or no, and why.** Answer it honestly. A yes is not a
problem; an unnoticed yes is.

- `gofmt`, `go vet` and `go test ./...` clean.
- Table-driven tests.
- Commit messages are three to five lowercase words, no prefixes and no
  punctuation.
- No em-dashes anywhere in code, docs or commit messages. CI enforces this.
- Never weaken a verdict to make a run look clean. If you suspect a harness
  bug, prove it with the reference pipeline and record it in `docs/decisions/`.

## Good first issues

Look for the `good-first-issue` label. The adapter and profile guides above are
the fastest routes into the codebase.
