# Launch plan

Where to post LossLedger, in what order, and what has to be true first.

A launch is a one-shot resource. The posts below work once. If they fire before
the gate passes, the top comment is "there is no README and nothing to run" and
there is no second attempt.

---

## 1. The gate: do not post until all of these are true

| # | Requirement | Why it blocks |
|---|---|---|
| G1 | **The repo compiles from a fresh clone.** `git clone && go build ./...` with no errors. | Checked 13 Nov 2026 and it did not: `token.go` was public, `hmac.go` was not, so `Sign` and `Equal` were undefined. The commit drip publishes by file, not by "each commit builds." |
| G2 | **README present**, with the alloy and MoJ framing, the hero image, a quickstart, and the limitations section. | Without it GitHub renders the Apache licence as the landing content. |
| G3 | **The engine is public**: emitter, verifier, at least one real sink adapter, one working compose demo. | `internal/wire` alone cannot be "used". |
| G4 | **One measured number of your own.** | This is the launch. The tool rides the number. |
| G5 | **v0.1.0 tagged** with release notes and prebuilt Linux binaries. | goreleaser does this in about twenty minutes. |
| G6 | **Employer OSS clearance** for the repo and for the posts. | PRD section 12. The repo went public on 13 Nov 2026 before this was confirmed. |

### On G4, the measured number

Everything else is table stakes. This is the post:

> [collector] at [default setting] lost [N] of [M] lines under [condition].
> Tuned to [value], zero loss. Reproduce with one command.

Without it the Show HN is "I built a harness", which lands around fifteen
points. With it the tool is evidence rather than an announcement.

Getting it needs M3: a compose testbed with one collector and the fault
injectors. One collector and one real store is enough for the first post.

### What the project's own PRD says

> The launch artifact is not the repo; it is the first published cross-collector
> loss matrix.

That is M4. M1 is the current state. Treat the first post as a finding, not as a
project announcement.

---

## 2. Firing order

Space these out. Simultaneous cross-posting reads as spam and gets filtered.

### Day 1

**Show HN.** Weekday, roughly 8 to 10am ET. Stay in the comments all day.
HN is the fountainhead; everything downstream quotes it.

Title the post on the finding, never on the project:

> Show HN: [collector] loses [N] of [M] log lines under default config, and nothing counts it

**r/devops**, a few hours later. Different framing: the operator's story, not the
builder's.

### Day 1 to 2

**r/kubernetes**, then **r/selfhosted**. One per day. Never the same hour.

### Day 2 to 3

- **lobste.rs**, tag `devops`
- **Grafana community forum**
- **Vector Discord** and **Fluent Bit Slack**

In the vendor channels, contributor tone only: "built a harness you can point at
your own configs". Never vendor tone. These are the people whose software the
findings are about.

### Day 3 and after

- **LinkedIn.** The infra crowd is already in the network.
- **dev.to** crosspost of the write-up.
- **A comment on the upstream issues**: grafana/alloy #7076, fluent-bit #2509
  and #2955. These pages are permanent search landing pages for people with
  exactly this pain, and they outlive any launch spike.

### Ongoing

One finding or re-test post per month beats any launch spike. Re-test posts
after an upstream fix are mandatory under the disclosure policy, and they are
the half of the loop that builds the alliance.

---

## 3. Drafts

Fill every `[bracket]` with a real measured value. Ship none of these with a
placeholder still in them.

### Show HN

> **Title:** Show HN: [collector] loses [N] of [M] log lines under default config, and nothing counts it
>
> I run ELK and ClickHouse pipelines, and I kept hitting the same thing: a line
> that should be in the store is not there, and every counter in the chain says
> delivery was fine.
>
> That is structural. A collector counts the drops it notices. The losses that
> hurt are the ones nothing notices: a pod that exits before the file-discovery
> interval fires, a rotation race, a line dropped because its timestamp went
> backwards, a document the store rejected after the agent counted it as sent.
>
> So I built a black-box auditor. An emitter writes sequence-numbered,
> HMAC-signed canary lines through your real pipeline in realistic shapes
> (multi-line stack traces, 64 KB lines, interleaved timestamps). A verifier asks
> each store which sequence numbers it actually holds, and reports exact gap
> ranges.
>
> First measured result: [collector] at [default setting] lost [N] of [M] lines
> under [condition]. Tuned to [value], zero. Repro is one command: `[command]`.
>
> The part I care most about: it must never accuse a pipeline wrongly. A sequence
> is only reported missing after an adaptive settle window and a mandatory
> re-check, and the tool exports `lossledger_false_missing_total`, which counts
> anything it called missing that later arrived. It is printed on every report
> and must be zero. Two reference pipelines ship in-tree, one correct and one
> sabotaged in known ways, and CI fails if the sabotaged one is not caught with
> exactly the injected ranges.
>
> Honest limits: most teams tolerate 1 to 2 percent log loss without noticing,
> because most log lines are never read. This is for teams where logs are legally
> load-bearing, platform teams, and people mid-incident. Logs only, Linux only,
> and it proves loss rather than repairing it.
>
> Live demo: https://lossledger-canary.vercel.app
> Code: https://github.com/Vishv07/Lossledger

Expect the top comment to be "how do you know your harness is not the thing that
is wrong". The refgood and refbad gate is the answer. Have it ready.

### r/devops

> **Title:** I measured how many log lines my pipeline actually delivers. The answer was not 100 percent.
>
> Every component in my log chain reported healthy. Lines were still missing.
>
> The reason is that in and out counters can only count drops a component knows
> about. Discovery races, rotation races, out-of-order-timestamp drops and ingest
> rejections are all invisible to them. There are public cases: grafana/alloy
> #7076 (199 of 5,035 entries silently dropped over 14 hours), fluent-bit #2509
> where pods stopped shipping for hours with clean counters, and a UK Ministry of
> Justice incident whose runbook ends "These logs have not been recovered."
>
> So I wrote a source-to-sink auditor. Numbered signed canary lines in, exact gap
> ranges out. Against [collector] on default config it found [N] of [M] missing
> under [condition].
>
> It is deliberately conservative about accusing anything: adaptive settle
> window, mandatory re-check, and a published metric counting any sequence it
> wrongly called missing.
>
> I would genuinely like people to point it at their own configs and tell me what
> it finds, including when it finds nothing.
>
> [link]

### Upstream issue comment

Highest leverage and most delicate. Contributor tone, no pitch.

> I hit this class of problem often enough that I built a harness to measure it:
> numbered, signed canary lines through the real pipeline, then query the store
> for which numbers arrived. Against [version] with [setting] I measured [N] of
> [M] missing under [condition]; tuned to [value] it was zero, repro attached.
>
> Posting in case it is useful for regression testing rather than as a bug
> report. Happy to run it against any config you want checked.
>
> [link]

`docs/disclosure.md` governs this one. Maintainers hear findings privately
first, config defaults are described as defaults and never as "X is broken", and
the owner files, not an agent.

---

## 4. Tone rules that apply everywhere

Taken from the project's own code of conduct and disclosure policy:

- Lead with the methodology, then the finding. Never the other way round.
- Praise what passed as loudly as what failed. A report that only lists failures
  is marketing, not measurement.
- Report default-configuration behaviour as exactly that, and show the tuned
  result beside it.
- No gloating, ever.
- Publish the integrity metric with every result. A number without it is not
  interpretable.

---

## 5. Pre-flight checklist

Run through this the morning of the post.

- [ ] `git clone` into a clean directory, then `go build ./...` and `go test ./...`
- [ ] `lossledger selfcheck` passes from the fresh clone
- [ ] README renders correctly on GitHub, image included
- [ ] The quickstart in the README works verbatim on a machine that has never run it
- [ ] Release binaries download and run
- [ ] The measured number reproduces 10 times out of 10
- [ ] Maintainers of any named project were contacted privately first
- [ ] Employer clearance confirmed in writing
- [ ] Two hours clear to sit in the comments
