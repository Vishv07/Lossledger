# Responsible disclosure and publication policy

LossLedger exists to find delivery bugs in other people's software. How those
findings are handled decides whether this project is a contribution or a
nuisance.

## What counts as a finding

A `MISSING` or `DUP_BUG` result against a named collector or store version that
reproduces **10 out of 10** times from a committed scenario, on default or
explicitly stated configuration.

Anything that does not reproduce 10 out of 10 is not a finding. Scenarios whose
verdicts vary across replays are labelled FLAKY, reported separately, and never
published.

## The process

1. **Maintainers first, privately.** The security contact if the project
   designates one, otherwise the issue tracker. The report includes the
   scenario, the seed, `versions.lock`, and a one-command reproduction.
2. **14 days, or until fixed or acknowledged, whichever is sooner.** During
   that window the public matrix shows the cell as "1 issue under disclosure"
   without the detail.
3. `ORDER` and `LATE` findings may be filed publicly straight away. They are
   bugs, not hazards.
4. **Configuration defaults are described as defaults.** A finding about a
   default cooldown is reported as "default-config behaviour", never as "X is
   broken", and the tuned-config result is shown beside it.
5. **Re-test posts after a fix are mandatory.** They are the half of the loop
   that builds the alliance.

## Who publishes

**The owner files and publishes. No exceptions.** The implementing agent drafts
findings into `docs/findings/` and stops there.

The repository stays private until the owner confirms employer open-source
clearance.

## Tone

Reports praise what passed as loudly as what failed. Methodology first,
findings second, zero gloating. A report that only lists failures is marketing,
not measurement.
