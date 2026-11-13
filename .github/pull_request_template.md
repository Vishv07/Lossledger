## What this changes

<!-- One or two sentences. -->

## Does this change what gets classified as MISSING or DUP?

**Yes / No**

<!-- If yes, say exactly how, and which gate test covers it. A yes is fine.
     An unnoticed yes is not. -->

## Checklist

- [ ] `go test ./...` passes
- [ ] `gofmt -l .` is empty and `go vet ./...` is clean
- [ ] The self-validation gate still passes (`lossledger selfcheck`)
- [ ] No em-dashes in code, docs or commit messages
- [ ] Commit messages are three to five lowercase words with no prefix
