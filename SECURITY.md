# Security

## Reporting a vulnerability in LossLedger

Open a draft security advisory on the repository, or contact the owner
privately. Please do not open a public issue first.

## What LossLedger touches

The emitter writes files inside directories you configure. It resolves that
root through symlinks once at startup and refuses every path that escapes it.
It never writes anywhere else, and a violation aborts rather than degrades.

The verifier is read-only against your stores by construction: the `Sink`
interface exposes no write method, so an adapter cannot modify the system it
audits even by mistake.

Neither component requires root.

## The signing key

`LOSSLEDGER_HMAC_KEY` is shared between the emitter and the verifier. It keeps
foreign or forged lines out of the ledger; it is not a confidentiality
boundary, and the 48-bit truncated signature on each line is not sized to be
one.

The key is read from the environment, never from a config file, so it stays out
of the files people paste into issues. It never appears in metrics, reports or
logs.

## Findings about other projects

Delivery findings about a collector or a store are not reported here. They go
to that project's maintainers privately first, under the policy in
`docs/disclosure.md`.
