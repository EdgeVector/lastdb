# Native cloud backup: first source wave

Status: source implementation, with no deployment or runtime activation.
The [shared contract](native-cloud-backup-protocol-v1.md) remains the complete release requirement.
This wave supplies B1, S1, and R1 source interfaces. It does not supply a complete cloud backup.

## Current source

| Package | Source result | Current limit |
| --- | --- | --- |
| B1 | Durable native cut, retained files, bounded object readers, and exact restart state | Complete-backup eligibility stays false until B2 certifies file dependencies |
| S1 | Conditional jobs, atomic transfer admission, fixed R2 targets, quota obligations, and verified receipts | New effects default off; S2 acceptance and recovery grants remain absent |
| R1 | Frozen accepted selection, account recovery, bounded import, native validation, and fresh destination publication | File dependencies cause refusal until B2 supplies their resolver |

The service imports the canonical crate through its existing fold submodule.
The native Mini refusal remains in place. No new command or provider adapter calls the importer.
The source does not change the primary, shared services, CI, routines, or provider configuration.

## Native file packing and capture

Backup capture uses the existing native disk-pack files and their exact stored bytes.
It does not create a second record layout.
Files of at least 64 KiB stream directly from the retained native source.
Smaller files share metadata objects of at most 8 MiB, with explicit member extents and hashes.
Backup capture releases the local write gate before hashes, metadata assembly, or network work.

The saved cut includes its CURRENT bytes, generation, frontier, native hold, inventory, and frozen transfer inputs.
A restart resumes that cut. It cannot substitute a newer root.
Backup capture saves the remote terminal result before it releases its native hold.
Open readers retain that hold until their release.
A missing source still permits an exact close request and terminal confirmation.

One process guard covers an active export or restore and all export readers.
Only one active operation can use the cloud memory allowance.
The limits cover cloud allocations. They do not cap the entire Mini process.

The Mini owner adapter must derive the RecoveryDescriptor from the actual source account and key context.
The low-level capture code cannot establish that authority from caller metadata alone.

## Service jobs and transfers

The service saves immutable job input and uses conditional transactions for job admission and close.
Close on an absent job creates a permanent fence against a delayed begin.
Status, close, and transfer evidence recovery remain available to the authenticated owner when new effects are disabled.
Current account authority still gates each new grant.

Each prepare batch belongs to one job and reserves its summed bytes through one atomic transaction.
Provider work starts only after the service saves the exact target and byte obligation.
The target includes the full frozen transfer digest.
The returned URL requires signed create-once and exact-length headers.
The service verifies stored bytes through a bounded stream before it saves a receipt.

A missing object, expired URL, or timeout does not erase a reservation.
Positive evidence converts the exact reservation to retained bytes.
Concurrent redundant objects keep separate physical-byte obligations.
The first canonical receipt keeps its original location.
No deletion or TTL resolves an unknown effect.

Native quota includes retained native bytes, unresolved reservations, and observed legacy use.
Before activation, legacy uploads require isolation or the same reservation ledger.
An old usage counter cannot bound concurrent legacy grants.
Some subscription and billing views still omit native counters.
The legacy purge path does not enumerate the native namespace and must remain excluded from its lifecycle.

The provider verification deadline is six seconds.
P1 must establish that deadline, maximum object sizes, and signed headers through real provider use.
Source checks cannot establish those facts.

## Restore and restart

The importer saves one AcceptedBackup, its job, and the fresh destination before any download.
It compares the service result with that selection and never follows a newer head on resume.
The recovery phrase follows the existing raw BIP39 entropy and account key derivation.
Each restore call checks the derived public identity against the selected account.

The importer verifies the initial manifest receipt, encrypted manifest, public reference tree, native inventory, exact bytes, and native account records.
Native file references without certified coverage cause refusal.
The importer exposes the destination only after native validation and required disk barriers.
A restart repeats the rename directory barriers before it records success.
The restored home keeps the account and native identity, leaves cloud sync off, and requires a fresh future stream.

## Source checks and next work

Format, build, strict Clippy, file size, function size, new lint exception, and byte-slice checks support source review.
No tests, fixture harnesses, primary access, cloud service calls, or runtime recovery checks occur in this wave.
The checks establish source consistency only.

The next source wave has two owners:

1. B2 defines atomic file dependency records, backup-owned blob copies, and the persistent restore resolver.
2. S2 verifies complete references and writes the sole accepted backup through one conditional transaction.

The integration owner then connects the Mini owner adapter and authorized service adapter through those interfaces.
P1 uses fresh synthetic data and normal product commands in an approved service environment.
Activation follows complete source work and product evidence, with Tom's separate approval for shared changes.
