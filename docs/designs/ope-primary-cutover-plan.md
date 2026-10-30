# Primary adoption of private-key-derived OPE

Status: Proposed plan, 2026-10-24. No primary change is authorized by this document.

The [candidate security review](ope-candidate-security-review.md) rejected the first carry-profile extension.
The replacement uses the published binary CLWW construction with a bounded PRF.
Independent review and the complete native route still precede activation.

The primary needs new readers, new writers, and an explicit data-format conversion.
A binary upgrade alone does not select the new OPE.
The [implementation plan](ope-range-keys-implementation-plan.md) defines the scheme and the affected product code.
This document defines the route from that work to the primary.

## 1. Current state and intended result

The installed primary CLI and daemon both report `0.23.4-pre.0-331-g0a9d8cf48`.
That source precedes the OPE candidate.
This observation identifies the installed pair; it does not establish the primary's stored encoding settings.
The final preflight must read those settings through a supported product operation.
The read-only product status on 2026-10-24 reports 31 logical keys and cloud sync off.
Treat the primary as nonempty; the reset notice does not authorize a reset or data loss.

[PR 365](https://github.com/EdgeVector/lastdb/pull/365) closed without merge after CI confirmed its extra suffix leakage.
The replacement source uses independent 128-bit CLWW output blocks and excludes the PRF maximum before bit addition.
It also supplies scoped molecule grants and authenticated original-range protection.
Those APIs remain disconnected from the serving format.
Tom explicitly approved the single CI-only crypto proof: "Allow this CI-only exception".
[PR 371](https://github.com/EdgeVector/lastdb/pull/371) merged after the proof, required CI, and benchmark completion passed.
Independent automated review reported no major issue; it is not a professional audit.
Other tests remain retired. Native activation remains gated.

After adoption:

- The same private identity seed derives a separate range encryption key.
- Each molecule, epoch, and blinded hash has its own ordered range terms.
- Exact reads and range reads use those terms on disk or in optional memory data.
- Each tip retains its original range under separate authenticated encryption.
- Hash blinding, run-file block encryption, and file blob encryption retain their current purposes.
- The active format and epoch come from stored metadata, with explicit compatibility checks.

This change does not require whole-pack encryption or the wrapper change in [PR 321](https://github.com/EdgeVector/lastdb/pull/321).
It does not activate cloud sync or direct browser queries.
Order, equality, length, and some prefix information remain visible in the searchable terms.
Independent cryptographic review must assess the exact candidate before activation.

## 2. Delivery packages

| Package | Result required before the next package |
|---|---|
| A. Candidate and format contract | Resolve the PR hold; complete cryptographic review; fix the full logical range-size contract |
| B. Native integration | Connect all readers and writers; protect original range copies; persist the format and epoch |
| C. Conversion and recovery | Add explicit old-to-new conversion, interrupted-conversion recovery, and compatible backup restore |
| D. CI release | Exercise the active codec through product operations; complete required checks and measurements; produce a matched macOS pair |
| E. Primary cutover | Obtain candidate-specific approval; convert the stopped source; validate the target; promote and verify it |

Packages B and C may use several PRs.
Keep the old format selected until the complete route is ready.
Do not allow a new writer to serve before every reader understands its output.
Review PR comments and CI after each publication, through merge or closure.

### A. Freeze the candidate contract

Review the exact byte representation, variable-length order, prefix bounds, private-key derivation, and scope separation.
Review integrity checks, key ownership, and the leakage of the published extension.
Resolve any review defect before activation.
Keep a scheme change explicit if that review rejects the candidate.

The replacement's hex form uses `256*(N+1)` bytes for `N` logical range bytes.
The current whole stored-key limit is 65,535 bytes.
Choose a versioned storage change that preserves every supported old logical range length.
Do not silently reduce the logical limit or truncate a key.
Reserve the final logical format number against accepted main, including concurrent format changes.

### B. Connect the native product

Use the existing identity seed inside the product; do not replace or export it.
Derive the range root with its separate purpose, then derive molecule, epoch, and blinded-hash keys.
Keep the existing hash and content key purposes separate.
Define access to an existing shared molecule before that molecule can enter the new format.
Do not derive its owner key from a recipient's unrelated private seed.
Either deliver the authorized scoped key grant or refuse that conversion explicitly.

Update exact reads, batch reads, prefix reads, bounded ranges, and cursor pages together.
Update the disk route, optional preload route, and final response filters with the same bound rules.
Update named alternate-key changes, deletes, retention, conflicts, replay, and maintenance readers and writers.
Protect original range copies in core-owned events and cloud payloads, or exclude them through an explicit supported boundary.
Keep authorized local app storage outside the core confidentiality claim.

Store the encrypted original range in each applicable tip and retained record.
Validate its scope and regenerated term before a response uses it.
Refuse missing or invalid protected values in the new format.
Persist the format and epoch; do not infer them from term length or an environment variable.
Refuse an unsupported format before normal reads or writes start.

### C. Build the converter and its recovery route

Use a stopped source and a separate, inactive target.
Read the source format explicitly and decode its old ranges through that format's decoder.
Validate each recovered range against its source key.
Refuse unknown formats, invalid source records, missing keys, and inconsistent embedded references.

Rewrite all affected range identities, including tips, generation records, delete barriers, conflicts, and retained control values.
Preserve atom identities, atom content, counts, write-order facts, and the selected generation's precedence.
Store protected original ranges in the new representation.
Do not mix old and new terms in one ordered stream.
Do not use a legacy term as an online fallback.

Use bounded batches and a durable conversion journal.
Bind the journal to the exact source state, target, identity, format, epoch, and converter revision.
An interruption must leave the target unavailable to normal apps.
Publish its ready state only after complete validation.
Define the supported resume procedure before release.
Retain an unchanged source until promotion and its recovery decision permit retirement.

Validate complete record transfer and reference relationships inside the converter.
Known-key sample reads alone do not establish completeness.
Bounded physical record traversal belongs to the offline converter; it adds no public query without a key.

Support restore of retained old backups through their explicit old format and the converter.
Support new-format backup restore with the same private identity and stored epoch.
Refuse a wrong identity or unsupported format before a partial result can serve.
An old executable must refuse the new home.

### D. Complete CI and prepare the release

Use fresh, owned DEV data in CI.
Do not copy the primary, start a candidate on its copy, or use the archived primary.
Run builds, lint builds, and measurements in GitHub Actions.
Keep local work to source, syntax, format, and documentation checks.
The single authorized crypto proof runs only in CI; other tests and replacement test harnesses remain prohibited.

Use supported product operations for the permitted operational observations:

- Exact and batch reads, prefixes, range bounds, and cursor continuation return the expected records.
- Empty ranges, shared prefixes, Unicode, case distinctions, and supported maximum lengths preserve their semantics.
- Optional preload and the disk route return the same selected records.
- Named alternate-key changes, deletes, retained conflicts, compaction, and reopen preserve the accepted state.
- Conversion resume and backup restore preserve complete native relationships.
- Wrong identities and incompatible formats produce explicit refusal.

Measure the active OPE route, including cold seeks, writes, disk size, memory size, and expanded key limits.
Require `ci-required`, `generated-evidence`, and `benchmark-complete` on the final product source.
Keep CI summaries and artifact links as the validation record.
Prepare any new shared workflow for Tom's approval before publication.

After the implementation merges, use the Mini upgrade artifact workflow for the exact accepted source revision.
Produce the matched `lastdb` and `lastdbd` macOS pair with checksums and source metadata.
Produce compatible converter tools from their exact source revisions.
Verify artifact retention before the cutover window.

## 3. Primary cutover plan

### Approval and preparation

The recommended route is a supervised, journaled logical migration into a new-format target.
It needs no filesystem clone of the primary home.
The reviewed native logical-migration manager is DEV source, not an installed OPE migration capability.
Its current generic record transfer does not perform this range-key conversion.
Accept and release the required OPE-aware manager and converter changes before primary use.

Tom's no-copy rule still covers primary-derived trial nodes.
The final approval must explicitly authorize the logical record transfer, target checks, and promotion for this exact candidate.
Do not treat this plan, an automated PR comment, or an earlier binary-upgrade approval as that authorization.
Keep the old primary archive untouched.
No primary schema install or brain, situations, or kanban use forms part of this plan.

Before approval, prepare these concrete items:

- The accepted source revision, matched binary checksums, converter checksums, and CI links.
- The exact source and target format, epoch, identity-preservation result, and supported recovery procedure.
- The primary baseline, supervisor configuration, target location, and durable journal location.
- The exact known-key read set and any disposable write key that Tom permits.
- A disk-space bound for the retained source, expanded target, temporary work, and compaction.
- The maintenance window, app-write hold, recovery boundary, and post-cutover observation period.

Bind approval to those items.
Refresh the plan if another task changes the primary baseline or the candidate.
Keep cloud sync off through this route; obtain separate approval for a later cloud change.

### Execute only after approval

1. Confirm the approved baseline and exclusive ownership of this maintenance operation.
2. Hold app writes through the supported supervisor and app controls.
3. Stop the source gracefully; require its durable flush result and closed sockets.
4. Acquire the supported offline maintenance lock and verify the final source state.
5. Convert records into the private target with the approved converter.
6. Validate complete transfer, range identities, references, and the selected generation.
7. Open only the authorized target; perform the approved known-key checks and disposable write.
8. Confirm compaction, clean stop, and reopen on that target.
9. Stop the target before promotion.
10. Retain the original source data and select the converted data through journaled, same-filesystem renames.
11. Install the matched pair through the supported supervisor cutover path.
12. Record the new-writes-possible boundary durably before the public socket can open.
13. Start one supervised primary daemon and verify its selected data, version, format, and epoch.
14. Repeat the approved reads and write; verify durability after a clean reopen.
15. Observe the agreed workload, memory, disk, and error signals through the approved period.

The data selection and executable selection span several steps.
The journal must recover an interruption between them; two separate renames are not one atomic operation.
The supervisor must not start an old executable against new data during those steps.
Do not force a stop or substitute a manual daemon start if the supported path refuses the cutover.

### Recovery and completion

Before the public socket can open, recovery may select the unchanged source and its matched old pair.
The approved procedure must account for private target writes and partial data or executable selection.
After the new-writes-possible boundary, do not automatically select the old source.
It cannot contain subsequent acknowledged app writes.
Use forward repair or an explicitly verified reverse conversion that preserves those writes.
Preserve the target and journal until that recovery decision is complete.

Completion requires more than a new version string.
The primary must report the new stored format and epoch and return correct keyed results from that format.
The accepted write must survive reopen, and the native writer and maintenance signals must remain healthy.
The identity remains unchanged, and cloud sync remains off.

Retained source files and old backups still contain the old range encoding.
The new format does not remove those old bytes.
Handle their retention and eventual retirement through a separate approved recovery and data-retention decision.
Do not claim that all retained data gains the new protection at promotion.

## 4. Next action

Complete independent review and CI for the replacement profile.
Then complete the native size contract before active reader and writer integration.
Prepare the conversion and release evidence before a request to change the primary.
