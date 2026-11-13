#!/usr/bin/env python3
"""Builds the document pages from the landing page's own chrome.

The docs live on the site rather than only in the repository, so a reader can
follow a footer link without needing access to the repo and without waiting for
a given file to reach the main branch. Run this after editing index.html so the
shared styling stays in sync.
"""
import re
import pathlib

HERE = pathlib.Path(__file__).parent
SRC = (HERE / "index.html").read_text()

CSS = re.search(r"<style>(.*?)</style>", SRC, re.S).group(1)
NAV = re.search(r'(<header class="nav">.*?</header>)', SRC, re.S).group(1)
FOOT = re.search(r"(<footer>.*?</footer>)", SRC, re.S).group(1)

PROSE = """
/* ---------- document pages ---------- */
.doc{padding:clamp(44px,6vw,80px) 0 clamp(60px,8vw,100px)}
.doc-wrap{max-width:760px;margin-inline:auto;padding-inline:var(--gutter)}
.doc h1{font-size:clamp(2.1rem,4.4vw,3.1rem);margin-bottom:18px}
.doc .sub{color:var(--muted);font-size:1.05rem;max-width:58ch}
.doc h2{font-size:clamp(1.3rem,2.2vw,1.65rem);margin:48px 0 14px;padding-top:26px;border-top:1px solid var(--rule)}
.doc h3{margin:30px 0 10px;font-size:1.02rem}
.doc p{margin:14px 0;color:var(--ink-2);line-height:1.72}
.doc ul,.doc ol{margin:14px 0;padding-left:22px;color:var(--ink-2);line-height:1.72}
.doc li{margin:8px 0}
.doc li::marker{color:var(--rule-2)}
.doc b,.doc strong{font-weight:600;color:var(--ink)}
.doc code{font-family:var(--mono);font-size:.88em;background:var(--paper-2);border:1px solid var(--rule);border-radius:4px;padding:1px 5px}
.doc pre{background:var(--ink);color:#E8EDE9;border-radius:9px;padding:18px 20px;overflow-x:auto;margin:20px 0}
.doc pre code{background:none;border:0;padding:0;color:inherit;font-size:.84rem;line-height:1.7}
.doc blockquote{margin:22px 0;padding:4px 0 4px 20px;border-left:2px solid var(--red);color:var(--ink);font-family:var(--serif);font-size:1.08rem}
.doc .tbl-wrap{margin:22px 0}
.doc table{min-width:480px}
.crumb{font-family:var(--mono);font-size:.78rem;color:var(--muted);margin-bottom:20px}
.crumb a{text-decoration:none}
.crumb a:hover{color:var(--ink);text-decoration:underline}
.note{border:1px solid var(--rule-2);background:var(--panel);border-radius:9px;padding:20px 22px;margin:24px 0}
.note.red{border-color:var(--red);background:var(--red-soft)}
.note p{margin:0;font-size:.95rem}
.note p+p{margin-top:11px}
.toc{border:1px solid var(--rule);background:var(--panel);border-radius:9px;padding:18px 22px;margin:28px 0}
.toc ol{margin:0;padding-left:20px;font-size:.93rem}
.toc li{margin:5px 0}
.toc a{text-decoration:none;color:var(--ink-2)}
.toc a:hover{color:var(--red);text-decoration:underline}
"""

SHELL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} | LossLedger</title>
<meta name="description" content="{desc}">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Serif:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' fill='%23F0F3EE'/><line x1='50' y1='14' x2='50' y2='86' stroke='%2312171A' stroke-width='7'/><line x1='14' y1='34' x2='86' y2='34' stroke='%2312171A' stroke-width='5'/><line x1='14' y1='62' x2='40' y2='62' stroke='%23B5362C' stroke-width='7'/></svg>">
<style>{css}{prose}</style>
</head>
<body>
{nav}
<main class="doc">
<div class="doc-wrap">
<p class="crumb"><a href="/">LossLedger</a> / {crumb}</p>
{body}
</div>
</main>
{foot}
</body>
</html>
"""


def build(slug, title, crumb, desc, body):
    nav = (NAV
           .replace('href="#top"', 'href="/"')
           .replace('href="#problem"', 'href="/#problem"')
           .replace('href="#evidence"', 'href="/#evidence"')
           .replace('href="#method"', 'href="/#method"')
           .replace('href="#status"', 'href="/#status"'))
    foot = FOOT.replace('href="#top"', 'href="/"')
    html = SHELL.format(title=title, desc=desc, css=CSS, prose=PROSE,
                        nav=nav, foot=foot, crumb=crumb, body=body)
    (HERE / slug).write_text(html)
    print(f"  {slug}  {len(html):,} bytes")


METHODOLOGY = """
<h1>Methodology</h1>
<p class="sub">How LossLedger reaches a verdict, and what that verdict does and does not mean.</p>

<div class="toc">
  <ol>
    <li><a href="#model">The accounting model</a></li>
    <li><a href="#absence">Why absence is not loss</a></li>
    <li><a href="#settle">The settle window</a></li>
    <li><a href="#integrity">The integrity metric</a></li>
    <li><a href="#counters">Why not use the collector's own counters</a></li>
    <li><a href="#dups">Duplicates</a></li>
    <li><a href="#attribution">Attribution, and its limits</a></li>
    <li><a href="#bisection">Bisection, and what verification costs</a></li>
    <li><a href="#clean">What a clean result means</a></li>
  </ol>
</div>

<h2 id="model">The accounting model</h2>
<p>Every canary event gets a sequence number, strictly monotonic within an epoch, starting at 1. The emitter is the only component that assigns them and it never skips. A gap in the ledger therefore always means the pipeline lost something, never that the emitter missed a number.</p>
<p>An epoch is a ULID minted when the emitter starts. A restart mints a new one, so a restart can never look like a gap in the previous epoch. On a clean shutdown the emitter writes a final line stating the last sequence it wrote; on a crash there is no such line, the epoch is marked truncated, and nothing past the highest observed sequence may be called missing.</p>

<h2 id="absence">Why absence is not loss</h2>
<p>A log pipeline is asynchronous. At any instant some lines are legitimately in flight: buffered in the agent, sitting in a broker, or written but not yet visible to a query because the store has not refreshed.</p>
<p>Calling those lines lost would be a false accusation, and a false accusation is worse than a missed detection, because it destroys the credibility that makes detection worth reporting.</p>
<p>So a sequence is only reported <code>MISSING</code> when four things hold:</p>
<ol>
  <li>It is at or below the epoch's expected high-water mark.</li>
  <li>The sink does not hold it.</li>
  <li>The settle window has elapsed since it was emitted.</li>
  <li>A mandatory re-query has run and it is still absent.</li>
</ol>

<h2 id="settle">The settle window</h2>
<pre><code>settle = clamp(max(sink_hint, observed_p99_latency * 3), 5s, max_wait)</code></pre>
<p><code>sink_hint</code> is the store's own consistency floor: Elasticsearch's refresh interval, ClickHouse's async insert flush, Loki's ingester flush. The adaptive term lets the window grow when the pipeline is genuinely slower than usual, so a backlog does not turn into a page.</p>
<p>Both inputs push the window outward only. Nothing can shrink it below the floor, which is what makes this property hold:</p>
<blockquote>Lengthening the settle window never creates new MISSING.</blockquote>
<p>That is checked as a property test, not merely asserted. The window is printed in every report, so a reader can judge whether a verdict was fair instead of having to trust that it was.</p>

<h2 id="integrity">The integrity metric</h2>
<div class="note red">
  <p><b>lossledger_false_missing_total</b> counts sequences this verifier declared missing that later arrived. It is the shame counter.</p>
  <p>It is exported from day one, printed on every report whether it is zero or not, and must be zero in every published run. A nonzero value means the harness accused a pipeline of losing data it went on to deliver, which is a bug in LossLedger, not in your pipeline. It is treated as higher priority than any feature work.</p>
  <p>You cannot publish a result from this tool without publishing this number beside it.</p>
</div>

<h2 id="counters">Why not use the collector's own counters</h2>
<p>Because they structurally cannot see the losses that matter. A component counts the drops it notices; the expensive failures are the ones nothing notices. A verifier that consumed collector self-reports as verdict input would inherit the blind spot it is supposed to cover.</p>
<p>Collector logs and metrics may be collected for a report. They are never used to compute a verdict.</p>

<h2 id="dups">Duplicates</h2>
<p>At-least-once pipelines duplicate after a restart. That is correct behaviour, so <code>DUP_REPLAY</code> is informational and never fails a run.</p>
<p>A duplicate is only a bug when it falls outside any restart window, or when the duplication factor exceeds the bound (three by default), because no amount of legitimate replay copies a record four times.</p>
<p>In gauntlet mode the harness knows exactly when it killed a collector, so the classification is exact. In production mode it uses a time-window heuristic and labels the result <code>heuristic</code>. <b>A heuristic DUP_BUG is never published as a finding.</b></p>

<h2 id="attribution">Attribution, and its limits</h2>
<p>With an emitter and sinks only, two conclusions are available:</p>
<ul>
  <li>The same sequences are missing at every sink, which points at the source side.</li>
  <li>Sequences are missing at one sink only, which points at that sink's path.</li>
</ul>
<p>That is the whole of it. Per-hop attribution across a broker needs a tap at the broker, which is v2, and every report carries the limit as a footnote.</p>

<h2 id="bisection">Bisection, and what verification costs</h2>
<p>Verifying 173 million sequences per day by listing them would cost more than the pipeline being audited. Instead:</p>
<ol>
  <li>Ask <code>CountRange</code> for a wide range. If the distinct count equals the width and nothing is duplicated, the whole range is verified in one query.</li>
  <li>Otherwise split and recurse.</li>
  <li>Fetch per-sequence detail only at leaves below the detail threshold, which defaults to 512.</li>
</ol>
<p>Expensive queries scale with the amount of loss, not with the volume of traffic. A clean pipeline costs about one query per source per sink per interval.</p>

<h2 id="clean">What a clean result means</h2>
<p>That this pipeline delivered these canary lines, in these shapes, under these conditions, within this settle window.</p>
<p>It is not a proof that your pipeline never loses anything. It is evidence about a specific population of lines over a specific interval. Every report carries a Limitations section naming what was not tested, and that section is not optional.</p>
"""

DISCLOSURE = """
<h1>Disclosure policy</h1>
<p class="sub">LossLedger exists to find delivery bugs in other people's software. How those findings are handled decides whether this project is a contribution or a nuisance.</p>

<h2 id="finding">What counts as a finding</h2>
<p>A <code>MISSING</code> or <code>DUP_BUG</code> result against a named collector or store version that reproduces <b>10 out of 10</b> times from a committed scenario, on default or explicitly stated configuration.</p>
<p>Anything that does not reproduce 10 out of 10 is not a finding. Scenarios whose verdicts vary across replays are labelled FLAKY, reported separately, and never published.</p>

<h2 id="process">The process</h2>
<ol>
  <li><b>Maintainers first, privately.</b> The security contact if the project designates one, otherwise the issue tracker. The report includes the scenario, the seed, the pinned versions, and a one-command reproduction.</li>
  <li><b>14 days, or until fixed or acknowledged, whichever is sooner.</b> During that window the public matrix shows the cell as "1 issue under disclosure" without the detail.</li>
  <li><code>ORDER</code> and <code>LATE</code> findings may be filed publicly straight away. They are bugs, not hazards.</li>
  <li><b>Configuration defaults are described as defaults.</b> A finding about a default cooldown is reported as "default-config behaviour", never as "X is broken", and the tuned-config result is shown beside it.</li>
  <li><b>Re-test posts after a fix are mandatory.</b> They are the half of the loop that builds the alliance.</li>
</ol>

<h2 id="who">Who publishes</h2>
<div class="note">
  <p><b>The owner files and publishes. No exceptions.</b> An implementing agent drafts findings and stops there.</p>
</div>

<h2 id="tone">Tone</h2>
<p>Reports praise what passed as loudly as what failed. Methodology first, findings second, zero gloating. A report that only lists failures is marketing, not measurement.</p>

<h2 id="security">Reporting a vulnerability in LossLedger itself</h2>
<p>Open a draft security advisory on the repository, or contact the owner privately. Please do not open a public issue first.</p>
<p>The emitter writes files only inside directories you configure, resolving that root through symlinks once at startup and refusing every path that escapes it. The verifier is read-only against your stores by construction: the sink interface exposes no write method. Neither component requires root.</p>
<p>The shared signing key keeps foreign or forged lines out of the ledger. It is not a confidentiality boundary, and the truncated signature on each line is not sized to be one. It is read from the environment rather than a config file, so it stays out of the files people paste into issues, and it never appears in metrics, reports or logs.</p>
"""

if __name__ == "__main__":
    print("building document pages")
    build("methodology.html", "Methodology", "methodology",
          "How LossLedger decides a log line was lost: the settle window, the mandatory re-check, and the integrity metric that must stay zero.",
          METHODOLOGY)
    build("disclosure.html", "Disclosure policy", "disclosure policy",
          "How LossLedger handles findings about other people's collectors and stores. Maintainers first, privately, with a reproduction.",
          DISCLOSURE)
    print("done")
