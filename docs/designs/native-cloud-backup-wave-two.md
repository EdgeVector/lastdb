# Native cloud backup: second source wave

Status: source integration, with cloud sync off.
The [shared contract](native-cloud-backup-protocol-v1.md) defines the release requirements.
The [first source wave](native-cloud-backup-wave-one.md) records the earlier source boundary.

## Current state

The public source API uses **backup capture** in place of **native exporter**.
Backup capture selects existing native pack files and retains their exact bytes.
Large native files stream directly. Small files share bounded upload objects with exact extents.
The stored format-one job and its existing path stay compatible across a restart.

This wave supplies B2 file coverage and S2 cloud acceptance source.
The [Mini integration wave](native-cloud-backup-mini-v1.md) supplies the worker, manifest assembler, service client, and product commands.
No source component starts cloud sync by itself.

## File coverage and recovery

The native atom batch now includes its exact file dependency records.
The same batch removes those records when the last atom reference goes.
The directory holds the whole pointer digest, file kind, original file identity, suite, and expected size.
The original encrypted pointer keeps the file key.

Backup capture checks each atom and its three known dependency keys at the retained root.
It also checks file references in each selected molecule record against that record's exact atom and directory.
The existing selection and delete rules choose the current record. Inactive records add no new coverage requirement.
Local file bytes already in native packs need no separate upload.
Capture authenticates those bytes under the original file key and checks their plaintext identity.

An external main file, thumbnail, or poster needs a backup-owned copy.
Capture first checks the local recovery directory, then the existing authorized download APIs.
One exact ciphertext copy stays on disk until all of its authoritative part receipts reach disk.
Each cloud part holds at most 64 MiB and keeps its own frozen transfer identity.
Unknown upload results retain that copy and those identities.
Several atoms with the same pointer and file kind share one capture entry and its receipt extents.

Restore saves external ciphertext under the restored home and records its exact pointer and kind.
Normal file reads use that directory after a native file miss.
A later backup can reuse that ciphertext without access to the original owner's cloud object.
Restore never changes the selected native root or file pointers.

The [file capture design](native-backup-file-capture-v1.md) gives the exact source and memory bounds.
The disk-stream path removes the former 16 MiB backup authentication guard and 32 MiB native-CAS allocation guard.
Native copy, envelope authentication, JSON extraction, and restored file proof use bounded buffers and private disk files.
The [shared resource policy](native-backup-resource-policy-v1.md) separates fixed native V1 format sizes from editable source resource values.
Native physical parts stay at eight MiB. External ciphertext still obeys the 64 MiB object and 512 extent limits.

The one GiB metadata allowance does not cap a private payload file; filesystem capacity admission protects payload and scratch space separately.
The normal local file API keeps its separate owner-configurable 16 MiB default.
Restore also limits external pointer and kind identities to 43,690 within an eight MiB allowance.
Files outside the actual format, extent, metadata, or disk bounds receive an explicit refusal.
Older pointer atoms without certificates still receive an explicit refusal.

No automatic migration or primary access occurs.

## Cloud acceptance and read access

The service saves exact public reference metadata before a conditional provider write.
It keeps unresolved byte obligations across timeouts, job closure, and authority changes.
Positive byte evidence can settle an old obligation without permission for a new effect.

A commit freezes its exact input and checks the complete public receipt tree in bounded steps.
The separate encrypted manifest receipt also requires verification.
One conditional transaction writes the immutable accepted backup, updates the sole stream head, and closes active admission.
That transaction checks current account authority, the predecessor, the active job, and the certified submission.

The service supplies accepted-backup discovery, head reads, and authorized read grants.
Each read grant requires exact membership in the selected accepted backup.
An incomplete newer job cannot hide the previous accepted backup.
No mutable provider head, automatic deletion, or required-object expiry exists.
Closed byte reservations remain reserved, including reservations with no issued grant.
This source has no automatic reservation release path.

## Review fixes

- Capture and restore share a nonempty native atom range. An empty upper bound could skip all atom checks.
- Each atom page uses one known native locator. It does not reopen all 1,024 locators per page.
- Bounded native reads reject an excessive located record before payload allocation.
- Native read work budgets include file, decoded, and returned bytes. Separate bounds cover retained memory.
- The account decoder releases intermediate buffers between its bounded phases.
- New decoded file key buffers and retained dependency key copies clear after use.
- Terminal cleanup repeats disk barriers and removes only the exact job's private dependency files.
- A disk task retains its source and work guards after cancellation. A source reader prevents concurrent copy removal.
- Verified local recovery ciphertext precedes an external download on the next backup.
- File references in a molecule record can differ from its shared atom. Capture and restore now check both before completion.

Dependency status uses durable counters and one active file.
The full capture status still checks native catalog page sizes.
The Mini worker must keep that cost outside frequent progress updates.

These findings come from source review. They do not establish a live cloud result.

## Next action

Review the [Mini integration wave](native-cloud-backup-mini-v1.md) before service activation.
It uses the shared types and the actual source account and keys.

Before activation, isolate legacy grants or move them to the same quota ledger.
Complete native usage views and exclude native objects from legacy purge paths.
Expose reserved bytes, retained bytes, the blocked job, and the last accepted backup through product status.
The API Gateway still exposes only the legacy sync route for this service.
The native route, activation configuration, and deployment require approved infrastructure work.
Keep new service effects and cloud deletion off.

P1 then uses normal product commands and fresh synthetic data in an approved service environment.
It must establish cloud-only recovery, restart recovery, unknown responses, close races, byte verification, signed headers, and runtime deadlines.
No tests, fixture harness, primary copy, service deployment, or live recovery check occurs in this source wave.
Tom's approval precedes shared service, infrastructure, deployment, routine, CI, or primary changes.
