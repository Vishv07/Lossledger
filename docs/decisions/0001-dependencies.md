# 0001: third-party dependencies

The PRD says to prefer the standard library and justify every dependency.
Three are used.

## RoaringBitmap/roaring

**Used for:** the per-stream sequence ledgers.

The PRD names roaring specifically. The sets are long dense runs with
occasional holes, which is the shape roaring compresses best. The alternative
matters at scale: 200 sources at 10 lines per second is roughly 173 million
sequences per day, and a hash set of those would not fit the 500 MB verifier
budget in PRD section 8.2.

## prometheus/client_golang

**Used for:** the metric set in PRD section 7.8.

The metric names are part of the contract, and operators scrape them with
existing tooling. Hand-rolling the exposition format to avoid the dependency
would be a worse trade: more code, and a format that drifts.

## gopkg.in/yaml.v3

**Used for:** shape profiles and, later, gauntlet scenarios.

Profiles and scenarios are data by design, so a contributor adds coverage
without touching the engine. YAML is what the surrounding ecosystem uses.

## Implemented rather than imported

**ULID.** About 90 lines in `internal/wire/ulid.go`. LossLedger only mints and
compares its own epoch identifiers and never parses a ULID minted elsewhere, so
the full specification is more surface than is needed. Implementing it keeps the
dependency count at three.
