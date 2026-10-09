# Schema service eval harness (Stage 2: canonicalization & order)

A composable test harness for the question: **"given these sample inputs, in
this order, what canonical schemas does the service end up with — and are they
good?"**

## The two-stage model (why this only covers Stage 2)

`input → schema` is two steps in two places:

1. **Stage 1 — Proposal (the node, LLM-driven).** `POST /api/ingestion/process`
   on a `fold_db_node` runs an LLM over your raw JSON and emits a *proposed*
   schema. Expensive, nondeterministic, token-costly.
2. **Stage 2 — Canonicalization (the schema service, stateful).** The node
   POSTs that proposal to `/v1/schemas`. The service decides — against
   everything already registered — whether to **merge** it into an existing
   canonical or **register a new one** (dual-signal similarity). This is the
   order-sensitive part: the same proposal merges or spawns-new depending on
   what arrived before it.

This harness drives **Stage 2 directly**: it replays a corpus of *proposals*
into a fresh registry, in different orders, and judges the result. It's cheap
and (re)orderable because it skips the LLM. Proposals can be hand-authored (as
in `corpus.json`) or, later, generated once by Stage 1 and cached here.

## What it judges (deterministic, no LLM)

Grounded in the `expected_concept` label on each corpus item:

- **Correct reuse / no dup explosion** — items of one concept should collapse
  to one canonical. More than one ⇒ dup explosion.
- **Faithful field capture** — the resulting canonical should retain the union
  of its members' fields (cross-checked with the reuse probe's `unmapped_fields`).
- **Right semantic identity** — a reuse must land on a *same-concept* canonical;
  a contact merging into a photo is a wrong-merge.

It also reports **confluence**: do all orderings of the same set converge to the
same canonical set, or is the outcome order-sensitive?

### Compositional reuse (decompose/apply)

The headline reuse metric this harness exists to move is **`composed`** /
**`composed_count`**: a record whose nested components each match a
*pre-existing* canonical should register as **COMPOSED** — its `ref_fields`
rewritten to point at the reused child canonicals via typed `SchemaRef`s —
instead of re-inlining every field into one fat mega-schema (which inflates
`max_canonical_fields` and drags `reuse_rate` down).

Because measuring that path is the whole point, the ephemeral service boots
with `SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply` **by default**.

A composite proposal (e.g. `corpus.json`'s `ecom_order_composite` /
`ecom_invoice_composite`) carries `ref_fields` mapping each nested field to the
component canonical it should reuse (`ship_to → "Postal Address"`), with a
field description carrying that canonical's purpose. With the apply path on, the
service decomposes the proposal, reuses each matched component via a typed
`SchemaRef`, and returns the **`SchemaAddOutcome::Composed`** outcome — surfaced
on the wire as **`AddSchemaResponse.composed: true`**. A component that doesn't
clear the per-component purpose gate (τ_purpose 0.88) stays in the residual.
`expected_decompose: [a, b, …]` on a corpus item is the ground truth: it
*should* compose across those components.

The harness scores COMPOSED **off that wire flag** (`addBody.composed`), NOT off
the mere presence of `ref_fields` — a proposal authored with `ref_fields` would
otherwise look "composed" even with the apply path off, since the response
echoes its `ref_fields` back either way. So `--no-compositional` (path off)
returns `composed:false` for the very same proposal, it scores NEW, and you get
the genuine A/B baseline:

```
node run.mjs --baseline=seeded                     # apply on:  composed 2/2 (components-then-composite order)
node run.mjs --baseline=seeded --no-compositional  # apply off: composed 0/2
```

(Composition is order-sensitive: a composite only composes when its component
canonicals were registered *before* it — so the `reversed` ordering, which
replays composites first, also reports `0/2` even with the path on. That's
correct: there's nothing to reuse yet.)

This is all dev-only and ephemeral — it never touches the :9001 brain or any
prod surface; flipping the dev flag on by default *is* the measurement, distinct
from the human-gated prod cutover (the `schema-decompose-apply-path` card owns
the engine).

> Note: the apply path is **nested-only** — it reuses components a proposal
> already declares through `ref_fields`. A flat proposal that *inlines* a
> would-be component's fields (no `ref_fields`) won't compose; clustering flat
> fields into candidate components is a separate, lower-risk follow-on.

## Run it

```bash
cd fold/schema_service/eval

# Stage 2 only, hand-authored proposals, seeded baseline (default):
node run.mjs

# Focused original-order replay for locked gate checks:
node run.mjs --orders=original

# Full real pipeline: real inputs -> LLM proposal (Stage 1) -> canonicalization,
# plus the LLM-as-judge:
node run.mjs --stage1 --judge

# Flags:
#   --stage1            generate proposals from each item's raw `input` via the
#                       real node LLM (claude-haiku-4-5), cached by input hash
#   --judge             run the LLM-as-judge over the original-order scenario
#   --baseline=seeded   942 schema.org/persona seeds present (default, faithful)
#   --baseline=empty    reset-only; user-vs-user, seeds wiped
#   --corpus=FILE       load an alternate corpus (e.g. a generated one)
#   --no-compositional  boot the service with the decompose/apply path OFF
#                       (flat baseline) — see "Compositional reuse" below
#   --keep-server       leave the ephemeral service up to poke at :9102
```

First run compiles `schema_service_server_http` and downloads the MiniLM
embedder (~30 MB, cached in `.cache/.fastembed_cache/`, reused after).
`--stage1` and `--judge` need `ANTHROPIC_API_KEY`.

### Generate a bigger corpus

```bash
node gen_corpus.mjs --per-concept=5 --out=corpus_generated.json
node run.mjs --stage1 --judge --corpus=corpus_generated.json
```

### Locked gates

For the schema match/decomposition cutover card, use the focused gate checker
instead of the broad `score.summary.pass` field. The broad pass still includes
older corpus-health goals such as zero dup explosion; the locked cutover gates
are false merges, expected decompositions, reuse-rate floor when supplied, and
Schema.org component-cover field coverage:

```bash
node run.mjs --orders=original
node component_cover.mjs --field-embeddings
node gates.mjs --min-field-coverage=0.86
```

`gen_corpus.mjs` asks the LLM for K diverse documents per concept (varying field
names/shapes for the *same* concept) and labels each — so the scorer has ground
truth at any scale. Stage 2 is ms/item, so reordering tens of thousands is cheap;
only Stage 1 (cached) costs tokens.

### The large-corpus baseline (`corpus_generated_64.json`)

`corpus_generated_64.json` is a committed, self-contained 64-item corpus (8 inputs
× 8 concepts, generated by `gen_corpus.mjs --per-concept=8` with each item's real
Stage-1 proposal baked in) used to **baseline schema-reuse behavior at scale**
before the decomposition apply path lands. Because the proposals are embedded it
replays without `--stage1` (no tokens):

```bash
node run.mjs --corpus=corpus_generated_64.json --judge   # → results/latest.json
```

The recorded before-numbers (reuse_rate ≈0.57, 64 inputs → 28 canonicals, ≈22
dup-explosion, composed 0/0, non-confluent across orderings) live in the fbrain
reference `schema-eval-baseline-2026-06-22` (linked from `schema-eval-results-log`).
Re-run the identical command after the apply path lands to fill in the after-row.

### Local resolver threshold validation

Before a node skips live `schema_service`, validate the local resolver policy
against the committed 64-item corpus and adversarial near-neighbor fixtures:

```bash
node local_resolver_validation.mjs --self-test
```

The command is deterministic and CI-safe: no LLM, no network, no FastEmbed model
download, and no primary LastDB/FoldDB state. It reports the all-service
baseline (`before`) beside the local-skip policy (`after`):

- service-call avoidance rate
- fallback rate
- local-decision count
- average field coverage
- full-record coverage
- false-positive count
- imported service-computed embedding, persisted-after-restart, and local
  recompute fallback latency estimates

The current recommended gate is intentionally conservative:

```json
{
  "schemaIntentNameSimilarity": 0.70,
  "fieldSimilarity": 0.58,
  "minCoverage": 0.75,
  "fullRecordCoverage": 1.0,
  "ambiguityMargin": 0.08,
  "schemaCandidates": 8,
  "fieldCandidatesPerSchema": 6,
  "componentCandidates": 4
}
```

Expected behavior: imported service-computed embeddings and persisted local
embeddings should be effectively progress-free for users. If a node has to
recompute proposal embeddings locally, show normal write/import progress until
the fallback finishes, then use live `schema_service` when the local gate cannot
clear these thresholds. The self-test includes note-vs-Trip near neighbors so
generic `note` fields do not incorrectly match Trip fields.

## The hourly routine (files improvement cards)

`routine.mjs` runs the eval, analyzes the result into a few stable improvement
**findings**, and upserts one fkanban card per theme (deterministic slugs, so
re-runs refresh the same card rather than duplicating). It FILES cards and
follows the board — it never ships code, never git-pulls, never touches the
fold working tree, and evaluates against an ephemeral service (never :9001).

```bash
node routine.mjs                      # dry-run: print the cards it would file
node routine.mjs --file               # run eval + file/refresh fkanban cards
node routine.mjs --file --from-results  # file from the existing results (no re-run)
```

Themes: dup-explosion, wrong-concept merge, persona-seed type collision (409),
and the LLM judge's clustered fixes (semantic-overlap-before-NEW,
field-overlap-before-REUSE, hierarchical / per-field canonicalization — i.e.
"schema splitting"). Proposals AND judge verdicts are cached, so steady-state
cost is ~0 — tokens are spent only when the service's behaviour changes.

### Tracking progress over time

Every routine run (and any `run.mjs --log`) persists its result three ways via
[log_result.mjs](log_result.mjs):

- `~/.schema-eval/history.jsonl` — append-only local source of truth (STATE).
- brain `schema-eval-results-log` (type `reference`) — a rolling trend table,
  newest-first, rebuilt from history each run. **This is the at-a-glance
  progress view:** `brain get schema-eval-results-log`.

Lower dup/wrong/err and higher reuse over time = the service is improving.
A failed brain write is non-fatal (history.jsonl always lands first, and the
table is rebuilt from it next run). Pass `--no-fbrain` to log locally only.

### Scheduled hourly (launchd) — post-portal RUN home

Do **not** run this from `~/code/edgevector/fold` (that path is a portal with
no product tree). Install the eval tree + a prebuilt `schema_service` into a
RUN home, with results in STATE:

| Bucket | Path |
|---|---|
| **RUN** | `~/.local/share/edgevector/schema-eval/` (eval JS + `bin/schema_service` at a pinned fold SHA) |
| **STATE** | `~/.schema-eval/` (`routine.log`, `history.jsonl`, `results/latest.json`, MiniLM cache) |
| **DEV** | fold worktree (`./bin/wt start …`) — only place that cargo-builds |

```bash
# from a fold DEV worktree
schema_service/eval/scripts/install-launchd.sh install --from "$(git rev-parse --show-toplevel)"
schema_service/eval/scripts/install-launchd.sh refresh --from "$(git rev-parse --show-toplevel)"  # when fold main moves
schema_service/eval/scripts/install-launchd.sh status
```

The LaunchAgent (`com.tomtang.schema-eval-routine`) sets `SCHEMA_EVAL_SERVER_BIN`
so the hourly run **never cargo-builds**. Card filing uses `~/.local/bin/kanban`.
`routine.sh` propagates node's exit status (a failed run is a non-zero
`launchctl lastExit`). Logs: `~/.schema-eval/routine.log`.

```bash
launchctl kickstart gui/$(id -u)/com.tomtang.schema-eval-routine   # run now
launchctl bootout  gui/$(id -u)/com.tomtang.schema-eval-routine    # stop/uninstall
tail -f ~/.schema-eval/routine.log
```

Ad-hoc from a DEV worktree still cargo-builds unless you export
`SCHEMA_EVAL_SERVER_BIN`. Pass `--no-brain` to skip the trend upsert.

## Safety

Ephemeral, isolated instance: throwaway Sled db per run, isolated `$HOME` and
fastembed cache. **Never** touches the :9001 brain, **never** the dev/prod
Lambda. Consistent with how the existing schema_service Rust tests stand up
state. Dev-only.

## Files

| File | Role |
|---|---|
| `server.mjs` | boots/teardowns the ephemeral schema_service (reset = empty baseline, restart = seeded) |
| `client.mjs` | thin HTTP client for the v1 API |
| `propose.mjs` | Stage 1 — faithful node-LLM proposal (raw input → schema), content-addressed cache |
| `harness.mjs` | `runScenario()` — replay an ordered sequence, record per-step decision |
| `score.mjs` | `scoreScenario()` + `confluence()` — the deterministic Stage-2 scorers |
| `judge.mjs` | LLM-as-judge over each decision (cached) |
| `gen_corpus.mjs` | synthesize a labeled corpus of diverse inputs at any scale |
| `analyze.mjs` | turn a result into stable improvement findings (one per theme) |
| `paths.mjs` | RUN/STATE path + kanban/brain/bin resolution |
| `routine.mjs` | the hourly driver: eval → analyze → upsert kanban cards |
| `routine.sh` | launchd wrapper (PATH + exit-status propagation) |
| `scripts/install-launchd.sh` | copy eval tree + prebuilt bin into the RUN home |
| `corpus.json` | hand-authored Stage-2 corpus (inputs + proposals + labels) |
| `corpus_generated.json` | LLM-generated labeled corpus used by the routine |
| `run.mjs` | orchestrator: boot → (Stage 1) → replay orders → score → (judge) → `results/latest.json` |

## Growing it

- **More inputs** → append to `corpus.json` (target thousands; Stage 2 is ms/item).
- **Real LLM proposals** → run Stage 1 (node ingestion) once per input, cache the
  emitted proposal into `corpus.json` keyed by input hash. Then reorder freely.
- **LLM-as-judge** → add a scorer in `score.mjs` that asks a model
  "is this a good schema for this input?" over `(input, resolvedTo, finalCanonicals)`.
- **Seeded baseline** → today scenarios reset to whatever `POST /v1/system/reset`
  leaves (reported as `baselineCount`). For a seed-populated baseline, restart
  the service per scenario instead of resetting (slower, more faithful).
- **Continuous** → wire `run.mjs` into a routine; diff `results/latest.json`
  against the prior run to catch regressions when the seeds/thresholds change.
```
