# Snapshot inclusion map

The map records which device events the files of one snapshot include.
It contains the producer's local position and every enrolled device's import position.
An enrolled device with no imported events appears at sequence zero.
The map contains no record values, private keys, or device enrollment authority.

The map belongs to LastDB client metadata.
Cloud stores the encrypted files and their receipts without knowledge of the map or disk format.
The cloud protocol, null descriptor, native disk format, and file encryption stay unchanged.

## Exact source

The existing capture retains the CURRENT file, descriptor, and run files from each native store.
The map reader uses those retained files after the write gate becomes free.
It does not read the current device status or the latest import positions.
Later writes, imports, and compaction cannot change the captured map.

The reader fetches the captured head, namespace registration, roster, and exact peer keys.
It checks the peer sequence total against the captured receipt count.
An absent head permits Disabled only when the captured roster and namespace registration are absent too.
Incomplete or inconsistent metadata causes refusal.

Each device entry contains the origin, original anchor hash, last included sequence, and hash at that sequence.
Entries use strict origin order without duplicates.
The producer appears exactly once.
Sequence zero requires the original anchor hash.
Sequence values use canonical decimal strings, which preserve the complete unsigned 64-bit range.

The producer signs the map with its existing private device key.
The signature uses the separate domain `lastdb-snapshot-inclusion-map-v1`.
It covers the version, producer, device entries, and exact native root.
That root binds both store identities, generations, frontiers, descriptors, and CURRENT hashes through the existing run profile.
The cloud job identity remains separate from this file identity.

## Encrypted metadata and resume

New snapshot client metadata uses version 5.
Version 4 remains the existing mutation-log root format.
Version 3 snapshots remain readable with unknown logical coverage.
An old snapshot cannot become certified empty coverage through an absent map.

A version 5 root requires an explicit `logical_inclusion` value:

- `{"state":"disabled"}` when the captured home has no logical stream.
- `{"state":"enabled","map":...}` when the captured home has a logical stream.

The existing root encryption protects the complete value.
The root also retains the exact native profile and file-page references.
The ordinary accepted inventory protects the encrypted root as an opaque file object.

New private capture state uses format 3 and saves the explicit value before metadata becomes frozen.
Resume compares it with the same retained native files.
It preserves the saved map, signature, ciphertext, object identities, and file set.
An old format 2 capture keeps its original version 3 metadata and unknown coverage.
Resume does not add a certificate to an existing frozen root.

Before restore publication, LastDB verifies the map signature and its native root.
It then compares all entries with the actual restored native readers under the existing owner locks.
This check needs no private device key.
A valid signature from another snapshot cannot substitute for the selected files.

## Bounds

- At most 33 device entries: the producer and 32 enrolled peers.
- At most 32 KiB for the complete encoded signed map.
- At most 16 KiB for each captured control record, checked before its body allocation.
- At most 65,536 total imported sequence receipts, as in the existing import contract.
- One additional MiB of work space reserved inside the existing 96 MiB backup allowance.
- The existing 256 KiB client-root plaintext cap remains in force.

The first count, byte, memory, or disk limit controls admission.
The device-entry decoder refuses excess entries before it retains another typed entry.
The map does not change the warm set or the normal read and write limits.

## Local operation and CI

The owner logical-sync route supplies local snapshot preparation, inspection, resume, and release.
These operations use the actual retained-file capture and encrypted client-root code.
They save one exact local request identity and make no cloud request.
Release requires the exact local intent, with no remote submission authority.
The existing backup controller excludes concurrent local and remote capture work.

CI uses fresh generated nodes for event imports, snapshot capture, later writes, restart, and encrypted metadata checks.
It verifies refusal for changed signatures, changed positions, and a certificate from another captured cut.
The CI summary and artifacts retain the results.
No primary data or archive enters these operations.

## Boundary

The map supplies evidence about one snapshot.
It does not install a foreign snapshot, copy a device key, enroll a writer, or remove a log.
Snapshot adoption and safe log removal need separate implementations.

## Read-only adoption plan

The owner can compare a candidate's signed metadata with an existing, unreleased local capture.
The `snapshot_plan` action takes that capture's `request_id` and a `candidate` object.
The candidate contains the selected `native_root`, paired `run_profile`, and optional `logical_inclusion` from its encrypted client root.
The response adds `adoption_plan`; other snapshot actions keep their existing response fields.
The action neither prepares a new capture nor changes the existing capture.

The plan binds both native roots, both map hashes, and the local device identity.
The local map must match its held native readers.
The candidate's V2 profile must reproduce its selected native root.
The candidate must have valid signed coverage and a producer already enrolled in that local cut.
Every candidate origin must already have the same pinned anchor in the local cut.
An unknown producer, unknown origin, changed anchor, or equal-sequence hash conflict causes refusal.
Unknown legacy coverage and Disabled coverage also cause refusal.

For a candidate behind the local position, one batch reads the exact common-sequence hashes from the held local files.
The planner uses authored metadata for local events and compact receipts for received events.
It does not read event bodies.
An absent candidate entry remains explicitly absent.
Its replay request conservatively starts after the original sequence-zero anchor.
A peer ahead of the local position requires a candidate-prefix proof at the local known sequence.
The plan refuses a candidate that claims more events from the local device than its held authored head.

Each uncovered interval names its original stream.
Local authored events can come from that device's retained stream.
Received events require their original signed stream or an authorized retained copy.
The local receipt proves a hash; it does not supply the received event body.
Native mutation-log files cannot replace these logical events across different native homes.

Every report sets `replacement_allowed` to `false`.
It names the remaining shared-base proof, candidate-file verification, local-cut revalidation, identity preservation, receipt-base rewrite, and atomic publication work.
It also names required own-tail replay, original-source tails, and candidate-prefix checks.
Capture can start on a populated home, so sequence zero never proves that a home is empty.
The inclusion maps alone do not prove a shared starting snapshot.
No report grants trust, permits log removal, or authorizes a later file replacement.

The existing 33-device, 32-KiB map, and 16-KiB control-read limits remain.
A larger authored metadata record causes a bounded refusal.
The report is capped at 32 KiB.
Only the new action permits a 40-KiB control request; existing control and import limits remain unchanged.
The plan uses the existing one-MiB workspace inside the 96-MiB backup allowance.
It opens no second native snapshot and saves no plan, identity, receipt, or adoption state.

CI compares actual captured maps on fresh devices.
It checks frozen-cut repeatability, verified older boundaries, candidate-prefix requirements, required source tails, and refused certificates.
It also checks a sequence-zero device with data from before enrollment.
Record values, native heads, source metadata, and private identities must remain unchanged by the plan.
