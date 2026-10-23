# Single-cursor partition reader review

Baseline: a02d25ed0337f9fe6ce01d7b6d68a71d547f2e72 (merged PR #274).
Implementation: e5e3a413dbd39035cb615de2f0c6e2b5394660d5.

One file changes: 103 Rust lines removed, 51 added, net 52 removed.

The old outer partition page planner generated a singleton TipLane vector, merged
that lane into a BTreeMap keyed by API range, computed a common frontier across
lanes, and stopped consumption at that frontier. Current codecs admit one fixed
encoding per molecule, so there is exactly one lane. The new reader returns that
ordered page directly and uses short-page exhaustion instead of a shared frontier.

Ordering argument: plain range storage uses the same UTF-8 bytes as API range
order. OpeV1 emits fixed-width words with constant high 24 bits per position and
the plaintext byte in the low eight bits; for one molecule/key this mapping is
injective, strictly order preserving, and prefix preserving. All nonempty ranges
share the first word's high 24 bits. The reader retains that subtree bound and
reads the unencoded empty slot separately, inserting it at the front only for an
initial OPE page. A cursor after empty starts at the nonempty subtree. Plain
empty slots are already in the ordered storage stream.

A full single-stream page has no rows beyond a competing stream's frontier. A
short page is exhausted. The consumer advances its exclusive API cursor for every
examined row, including tombstones; only visible rows decrement offset or count
toward limit. The previous 128..1024 batch bound and cross-hash cursor fallback
remain. A zero limit still returns no rows without entering the loop.

The actual two-source merge inside scan_logical_tip_page is intentionally retained:
live records and immutable generation slots are real independent sources. That
code, generation deletion markers, captured changes, physical source receipts,
and preload window selection are unchanged. The preload route serves as a control
configuration rather than evidence that the changed disk route ran.

No blocking correctness finding in this self-review. Fixed current encodings and
valid native keys are prerequisites; arbitrary corrupt or mixed historical key
spellings are not a supported storage format. Benchmarks do not establish
power-loss behavior, concurrent generation activation, or exhaustive CAS behavior.

## Validation and runtime evidence

Both optimized lastdbd builds passed. Full workspace non-ONNX clippy passed with
warnings denied, as did formatting and source-size/no-new-allows checks.
All 24 matrix and 8 focused benchmark runs passed integrity checks. Added runtime
coverage includes offsets across batch bounds, exclusive cursors, deleted-row
refills, complete partition walks, and an independent empty/NUL/Unicode partition.
No test suite was added or run, per repository instructions. Artifact, whitespace,
and Python syntax checks are recorded with the review. See RESULTS.md for the
initial cursor slowdowns and the separate longer follow-up; the report does not
claim a universal speedup. Recommendation: keep for the simpler single-source code.
