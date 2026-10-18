# Native cutover validation

Full API workflow product commit: `a60b27d43f11fd6d1f3cf514d1579229d2256d5c`.
Git tree: `9c8d8dd75a5d2ebe8848b04808796db2ea87a386`.
Release SHA256: `bef1ff5b9047e8f76f2b42ec5f581bc385467d99785b6325a5a56a6d3f496f5a`.

The [final identity](candidate-final-identity.json) records the exact release command and host inputs.
The [committed-source check](committed-final-source-verification.json) matches all 1,883 captured product inputs to the exact committed tree.
The release uses normal default features and no storage feature flag.
The release build passes in 1 minute 51 seconds.

The final product commit is `436cc86ad82c4ca2486e0b7f55e4a1b840dfeaed`.
Its [release identity](response-final-identity.json) verifies all 1,883 product inputs.
Its release SHA256 is `66879950810b0c9b09756ff1ed0d6981b3ce3af849a89ab3436d7f23bf880045`.
The [final committed-source check](committed-response-source-verification.json) also matches all 1,883 inputs to that exact commit.
The release build passes in 57.48 seconds.
The [product delta](response-product-delta.json) confirms only the public error response's selector list changes.
Final format and workspace clippy pass again. The clippy result is 11.83 seconds, with warnings denied.
The full API workflows remain evidence for their recorded executable. The static list change receives a separate ordinary API check.
That [final check](accepted-response-v2/acceptance.json) passes: seven selector errors and three exact supported-selector lists.
The node uses its normal default engine and stops normally.
The final refusal driver identifies 24 inputs, with SHA256 `4b8e275bad1e0d6478698d46a8a53b554f0ada0b4f6850b8cfb37900c38510df`.

## Source and build checks

| Check | Result |
|---|---|
| Full workspace library and binary clippy, default lane | Pass, 37.52 seconds, warnings denied. |
| Separate schema worker and seed-generator binary clippy | Pass, warnings denied. |
| Workspace format | Pass. |
| File and function size, against merged main `011b29f3` | Pass, 1,054 changed files examined. |
| New allow attributes | Pass, zero added and three removed. |
| Build artifacts and whitespace | Pass for the committed source. Final receipt commit requires its own check. |
| Write-path synchronization source check | Pass. |
| Workspace dependency and clippy scope checks | Pass. |
| App registry source check | Pass. |
| SDK typecheck, lint, and build | Pass. SDK source stays unchanged during the final Rust-only fix. |

Exact check outputs reside in [validation](validation/).
The checks do not establish a live product result.
No tests, test suite, private fixture program, or coverage gate ran.

## Ordinary API scope

The accepted [V7 driver](driver-identity-v7.json) identifies 22 exact input files.
Its input SHA256 is `898f396743ad339e3873318d0287ba094a0edb4b0f272e4cbc6488b7745e3ca0`.
Each workflow uses a fresh owned synthetic home and the verified private release.
Every start removes engine-selection, memory-mode, replay, and binary-content overrides.
The normal format marker must exist after boot.

All three ordinary API workflows pass on the product commit above.
The tracked acceptance summaries cover [logical operations](accepted-logical/acceptance.json), [proteins](accepted-protein/acceptance.json), and [named databases](accepted-scoped/acceptance.json).
They record 124 keyed value checks, including 66 required zero-file checks, and ten normal node sessions.
Literal-prefix values add six-record checks before and after reopen.
Four unique atoms also return 404 after purge and after reopen.
The [results report](RESULTS.md) distinguishes logical absence, UUID-addressable absence, and disk-byte reclamation.

V5 and V6 stop at driver response-shape errors. Their receipts stay unchanged.
V7 corrects the error decoder and uses the existing logical key comparison.
Product source stays unchanged across those attempts.
The [plan](PLAN.md) records the exact corrections and required gates.
The first narrow final-response wrapper fails during receipt preparation, before any node starts.
Its corrected wrapper uses the same final product executable. The preparation failure stays separate from the accepted result.

## Evidence limits

The b391 release remains unrun discovery evidence. Its source and executable differ from the final release.
The earlier V6 benchmarks remain evidence for their earlier product commit only.
This cleanup contains no new before-and-after performance comparison.

True cached duplicate identities and the internal empty-transaction branch receive source review only.
The public route creates fresh mutation clocks and rejects empty public arrays before the core transaction.
Direct embedded helper entry, malformed key-wrap refusal, and old-format boot refusal also receive source review only.
No injected failure or cancellation program replaces those missing public triggers.

Cloud transfer and local replay stay disabled.
The primary, archive, brain, situations, and kanban stay unused.
