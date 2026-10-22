# Native cloud backup: second source wave

Status: source integration, with cloud sync off.
The [shared contract](native-cloud-backup-protocol-v1.md) defines the release requirements.
The [first source wave](native-cloud-backup-wave-one.md) records the earlier source boundary.

## Current state

The public source API uses **backup capture** in place of **native exporter**.
Backup capture selects existing native pack files and retains their exact bytes.
Large files stream directly. Small files share bounded upload objects with exact extents.
The stored format-one job and its existing path stay compatible across a restart.

This wave supplies B2 file coverage and S2 cloud acceptance source.
The Mini worker, manifest assembler, authorized service client, and normal product commands remain incomplete.
No source component starts cloud sync by itself.

## File coverage and recovery

The native atom batch now includes its exact file dependency records.
The same batch removes those records when the last atom reference goes.
The directory holds the whole pointer digest, file kind, original file identity, suite, and expected size.
The original encrypted pointer keeps the file key.

Backup capture checks each atom and its three known dependency keys at the retained root.
Local file bytes already in native packs need no separate upload.
Capture authenticates those bytes under the original file key and checks their plaintext identity.

An external main file, thumbnail, or poster needs a backup-owned copy.
Capture first checks the local recovery directory, then the existing authorized download APIs.
One exact ciphertext copy stays on disk until its authoritative verified receipt reaches disk.
Unknown upload results retain that copy and its frozen transfer identity.
Several atoms with the same pointer and file kind share one capture entry and one receipt.

Restore saves external ciphertext under the restored home and records its exact pointer and kind.
Normal file reads use that directory after a native file miss.
A later backup can reuse that ciphertext without access to the original owner's cloud object.
Restore never changes the selected native root or file pointers.

The [file capture design](native-backup-file-capture-v1.md) gives the exact source and memory bounds.
The current per-file plaintext limit is 16 MiB.
Larger files and older pointer atoms without certificates receive an explicit refusal.
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

## Review fixes

- Capture and restore share a nonempty native atom range. An empty upper bound could skip all atom checks.
- Bounded native reads reject an excessive located record before payload allocation.
- Native read work budgets include file, decoded, and returned bytes. Separate bounds cover retained memory.
- The account decoder releases intermediate buffers between its bounded phases.
- File key copies use zeroed storage after use.
- Terminal cleanup repeats disk barriers and removes only the exact job's private dependency files.
- Verified local recovery ciphertext precedes an external download on the next backup.

These findings come from source review. They do not establish a live cloud result.

## Next action

Connect the Mini owner, manifest assembler, capture worker, and service client through the shared types.
Derive the recovery descriptor from the actual account and keys.
Expose clear product refusals for unsupported files and uncertified old pointers.

Before activation, isolate legacy grants or move them to the same quota ledger.
Complete native usage views and exclude native objects from legacy purge paths.
Keep new service effects and cloud deletion off.

P1 then uses normal product commands and fresh synthetic data in an approved service environment.
It must establish cloud-only recovery, restart recovery, provider byte verification, signed headers, and runtime deadlines.
No tests, fixture harness, primary copy, service deployment, or live recovery check occurs in this source wave.
Tom's approval precedes shared service, infrastructure, deployment, routine, CI, or primary changes.
