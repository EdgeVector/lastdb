# Private-key-derived OPE implementation plan

Historical research: the portable OPE prototype and its CI proof are retired.
The accepted [typed-range field contract](typed-range-field-contract.md) uses no OPE.
The design below records the earlier candidate; it is not the current implementation plan.

Status: Portable replacement merged in PR #371, 2026-10-24. Native activation remains gated.

[PR 365](https://github.com/EdgeVector/lastdb/pull/365) closed without merge after its suffix-leakage counterexample passed in CI.
The [security finding](ope-candidate-security-review.md) remains the record for that rejected carry profile.
Keep the user's OPE requirement for the replacement assessment; do not silently accept the rejected leakage.
The [replacement options](ope-replacement-options.md) record the other costs and choices.

## 1. Replacement contract

Use binary CLWW OPE from the [published paper, Section 3.2](https://saweis.net/pdfs/pore-clww.pdf).
Instantiate its prefix PRF with independent 128-bit blocks and bounded rejection of the maximum block.
Addition of a plaintext bit then fits in that block, with no suffix carry.
The [crate contract](https://github.com/EdgeVector/lastdb/blob/b2419f5a8b510306411bb9c84a2fece0019ceec3/lastdb_range_crypto/README.md) specifies the exact profile and its review argument.
The profile is `lastdb:clww-binary128-nonwrapping:hmac-sha256:v1`.

Derive its root directly from the private identity seed with a separate purpose.
Derive one molecule secret from the root, exact molecule identity, format, and epoch.
Derive an OPE key for each stored blinded hash.
Keep existing hash blinding, run-file block encryption, and file blob encryption.

The current portable source includes deterministic full-byte terms, prefix bounds, protected originals, and scoped grant codecs.
The native serving selector, molecule bundles, and home marker still select the accepted old format.
No source API alone constitutes native activation or a primary upgrade.

## 2. Cryptographic review gate

Review the exact bounded-PRF instantiation against the published CLWW model.
For `Q` random-function prefix evaluations, its distance from uniform is bounded by `Q/2^128`, plus the HMAC PRF advantage.
Sixteen excluded draws produce an explicit error, never a misordered term.
Review the variable-length marker, unique PRF inputs, scope derivation, original-range integrity, and scoped grants.
Do not inherit a vendor extension's security claim or describe this as order-only encryption.

The expected leakage includes order, equality, length, and the first differing plaintext bit position.
Known distributions and correlated data can still permit value inference.
Independent review must accept this exact profile before native activation.
A behavior proof and a clean compiler do not satisfy that gate.

Tom explicitly authorized one CI-only crypto proof on 2026-10-24: "Allow this CI-only exception".
Reuse that one program for the replacement crypto contract; other tests and replacement test suites remain retired.
Use supported product operations for native operational evidence.
Run all LastDB builds, lint builds, and measurements in GitHub Actions.

## 3. Complete native size contract

Raw terms use `128*(N+1)` bytes and hex uses `256*(N+1)` for `N` logical bytes.
An empty HashRange range has a real term; the hash-only empty slot remains a distinct key type.
The default native whole stored-key cap remains 65,535 bytes.
Native feature 1024 adds explicit 32 MiB capacity for fresh targets; it does not select the new range codec.

Preserve the complete supported logical range length of every accepted source encoding.
Account for molecule and hash framing, scopes, generation keys, barriers, retention identities, and other embedded keys.
Choose a versioned native key limit and physical feature fence before a new writer can serve.
Audit external key extents, transfer, restore, resource quotes, and read budgets against that limit.
Do not silently reduce the limit, truncate a range, or replace it with a narrow integer or hash.
Reject an oversized write before its ack.

Measure the active representation's cold read, write, flush, memory, and disk costs in CI.
The larger keys require an explicit review of those measurements before primary cutover.
The normal daemon benchmark cannot measure an inactive codec.

## 4. Stored ranges and key access

Persist the selected format and epoch in explicit molecule metadata and the home's compatibility boundary.
Do not select an existing home's format through an environment variable or a token-length guess.
Keep terms from different epochs or codecs out of one ordered stream.
Allocate the final logical format number against accepted main.

Retain the outer tip-key frame:

```text
mk:{molecule}:{escaped_blinded_hash}\0{lowercase_hex(ope_term)}
```

Store the independently encrypted original range in `PerKeyRecord` and applicable retained records.
Bind the original to its molecule, hash, term, format, and epoch.
Validate its regenerated term before a response uses the original.
A new-format record with a missing or invalid protected field produces refusal.

Preserve the existing random molecule bundle's content and blind keys.
Add protected access to the derived range molecule secret through the scoped grant codec.
A shared molecule uses the owner's authorized secret; never derive it from a recipient's unrelated private seed.
An unsupported or unauthorized shared source must refuse conversion explicitly.
Freeze prepared ciphertext for each write retry.

Protect private original-range copies in core-owned conflicts, change events, replay markers, and cloud payloads.
Do not restore the generic storage wrapper as a shortcut.
Authorized local app files, including the local search inbox, remain a separate privacy boundary.

## 5. Readers and writers

The [inventory](encrypted-range-keys-reader-writer-inventory.md) identifies 18 reader families and 19 writer families across 80 explicit source references.
Its source links are immutable; its earlier mandatory-preload impact descriptions do not select the new design.
Refresh each family against accepted main before its implementation package.

| Route | Required result |
|---|---|
| Exact and batch keys | Derive one term per named key; resolve tips and atoms through existing batch routes |
| Prefix and bounded ranges | Normalize one hash's encrypted interval; preserve inclusive and exclusive rules |
| Cursor continuation | Bind the cursor to the same format, epoch, hash, and query |
| Optional ready-memory selection | Use equivalent bounds and term order; recover selected originals from authenticated records |
| Disk fallback | Return equivalent results without complete preload or a whole pack-file fetch |
| Copied fields and received sources | Use each authorized source's compatible molecule grant and stored format |
| Protein changes and conditional writes | Preserve exact-key checks and the related molecule publication boundary |
| Deletes, retention, and maintenance | Preserve barrier identities, expiry checks, counts, and durable selection precedence |
| Replay, conflicts, and change events | Preserve order facts and protect retained private key copies |

Do not introduce a public query without a key.
Keep preload optional; absence, refusal, invalidation, or timeout must select the equivalent bounded disk route.
A used schema does not authorize preload of unrelated fields or hashes.
Do not activate a writer before every serving reader understands its output.

## 6. Conversion and restore

Use an explicit offline converter with an owned, separate, inactive target.
Trials use fresh DEV data in CI; do not clone the primary or use its archive.
The generic native record transfer does not rewrite OPE identities.
Primary record transfer and target checks require separate candidate-specific approval.

Read the source format and molecule bundles explicitly.
Recover old ranges through that format's decoder and validate their source keys.
Use the stored blinded hash as context where the original hash is unavailable.
Rewrite tips, generation records, barriers, retention identities, conflicts, and every embedded range reference.
Preserve atom identities, contents, counts, write-order facts, and selected-generation precedence.
Do not retain legacy terms as an online fallback.

Use bounded batches and a durable journal bound to the exact source, target, identity, epoch, and converter revision.
An interruption leaves the target unavailable to apps and the source unchanged.
Validate complete transfer and key relationships before the target's serving marker can publish.
Define resume and recovery before release.
Old programs must refuse the new format.

Restore must support retained old backups through their explicit old reader and converter.
New-format restore must preserve identity, epoch, protected originals, and complete native relationships.
Wrong keys, partial targets, and unsupported formats produce refusal before service.
Old backups and retained files keep their old range protection until their approved retirement.

## 7. Delivery and primary cutover

| Package | Result |
|---|---|
| A. Portable replacement | Exact byte contract, bounded PRF, scoped keys, protected originals, authorized CI proof, and independent review |
| B. Native integration | Versioned full-range size support, all readers and writers, persisted format and epoch, and scoped key access |
| C. Conversion and restore | Complete offline rewrite, compatible backup restore, resume, and recovery boundaries |
| D. Release | Required CI, operational results, active-codec measurements, and matched macOS CLI/daemon/converter artifacts |
| E. Primary | Candidate-specific approval, supervised clean stop, validated target, journaled promotion, reopen, and observation |

Packages B through D keep the accepted serving format selected until the complete route is ready.
Product PRs require `ci-required`, `generated-evidence`, and `benchmark-complete`.
Keep raw output outside Git and link CI summaries and retained artifacts in each PR.
Review comments and CI after every publication; continue until each PR merges or closes.

The [primary cutover plan](ope-primary-cutover-plan.md) defines the exact approval, maintenance, and recovery boundaries.
Keep primary approval separate from PR completion.
Keep the wrapper change in [PR 321](https://github.com/EdgeVector/lastdb/pull/321) separate from range-format activation.
Do not change the primary or enable cloud sync through a library PR.

## 8. Future browser and cloud route

An authorized browser receives a scoped molecule secret, blinds the hash, and creates exact terms or bounds.
A future service follows ordinary ordered native keys without the private range key.
The browser authenticates original ranges and atom content after a fetch.
It needs no complete range preload or complete pack-file download.

The service still needs authorization, coherent revisions, freshness, resource bounds, and response-completeness rules.
OPE and its grant codec do not supply those properties.
Design and approve that service separately from local activation.
