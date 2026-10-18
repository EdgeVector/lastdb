# Final merge candidate: native core clarity

The final source cleanup passes all three fresh ordinary API workflows.
Normal startup selects the native engine without a mode or engine flag.
Cloud activation stays disabled. The primary and its archive stay untouched.
The earlier sealed receipts and evidence index remain unchanged.

## Exact candidate

| Item | Identity |
| --- | --- |
| Product commit | `5419748a0e8ebdc7044f3420808bbd280b6bb249` |
| Product tree | `847d73dd54dadb2bd301ff21565712fda443873a` |
| Release SHA256 | `38f7ac6b7ddaa1a03ebec3219355b2c1962322b90bc1c3b3093475355cd512a3` |
| Captured product inputs | 1,882; all match the manifest, current source, and committed Git blobs. |
| Ordinary API driver | Frozen V7, 22 inputs; SHA256 `898f396743ad339e3873318d0287ba094a0edb4b0f272e4cbc6488b7745e3ca0`. |

The [release identity](merge-identity.json) records the exact build, host settings, and previous identity chain.
The [Git verification](merge-committed-source-verification.json) checks every explicit product input.
The [post-workflow verification](validation/merge-post-workflow-verification.json) confirms unchanged source and executable bytes.
The normal default-feature release build passes in 1 minute 51 seconds.

The final source removes unreachable historical query branches behind the existing public current-value guards.
The public schema, query, count, and field boundaries still refuse unsupported layouts, selectors, and `as_of` requests.
The active docs now describe native startup, core memory, disk-first exceptions, and disabled cloud activation.
Historical mode measurements remain explicitly marked as historical evidence.

## Ordinary API results

| Workflow | Keyed checks | Required zero-file checks | Normal node sessions | Result |
| --- | ---: | ---: | ---: | --- |
| [Logical](merge-logical/acceptance.json) | 86 | 39 | 6 | Pass. |
| [Protein](merge-protein/acceptance.json) | 14 | 9 | 2 | Pass. |
| [Named database](merge-scoped/acceptance.json) | 24 | 18 | 2 | Pass. |
| Total | 124 | 66 | 10 | Pass. |

The logical workflow covers create, partial update, mixed changes, separate delete and recreate, ranges, concurrent complete values, flush, and reopen.
It also covers conditional writes, stale-condition conflicts, repeated public writes, and unsupported-operation refusals.
Six literal-prefix records preserve `ENC:`, `ENZ:`, and `ENB:` values and atom identities through normal reopen.
Their first reads require zero file reads.
Four selected unique atoms return public UUID 404 after warm purge and after reopen.
Cloud On returns its disabled error and leaves the known local intent files unchanged.

The default queued-create sample precedes its first value read.
It reports accepted sequence 15, completed sequence 13, one pending accepted batch, and 512 dirty logical paths.
The result proves a memory ACK for that request. Other queued writes can wait for disk.
The protein workflow verifies shared atoms, partial updates, member key moves, independent deletion, and reopen.
The named database workflow verifies scoped changes and independent personal values.

The [receipt seal](merge-receipt-seal.json) records exact sizes and hashes for 153 files across the three workflows.
Raw JSONL, node logs, and duplicate Python inputs stay local under the existing ignore policy.
Tracked acceptance summaries preserve the result without those raw payloads.
Small nested JSON identities follow the existing Git policy.

## Validation and limits

Full workspace clippy passes in 36.36 seconds with warnings denied. The separate off-lane clippy pass also succeeds.
Format, file size, function size, new-allow, and source gates pass.
The [validation directory](validation/) retains the exact check and release outputs.
No tests, fixtures, new benchmark, primary operation, installation, or deployment forms part of this proof.

Repeated public payloads receive fresh clocks. They do not exercise an internal idempotency hit.
The public empty batch refuses before core; the internal empty-transaction branch remains source-reviewed.
Direct embedded helper boundaries and old-format startup refusal remain source-reviewed.
The purge result proves addressable absence, rather than reclaimed disk bytes.
The record and byte allowances do not define a whole-process memory maximum.

## Owned temporary data

The [cleanup review index](merge-cleanup-review-index.json) identifies only three generated run roots and one private program container.
All ten node sessions stop normally. Every exact-path cleanup preview passes.
The [independent review](merge-root-review.json) approves only those four exact roots.
The supported reclaim removes all four roots after refreshed safety checks.
The [post-cleanup verification](merge-cleanup-verification.json) confirms all 153 new sealed file sizes and hashes remain unchanged.
It also verifies the older 357 sealed files and compares the old seals and evidence index with commit `f5de8fd0`.
No temporary database home or private executable copy remains from this final proof.
