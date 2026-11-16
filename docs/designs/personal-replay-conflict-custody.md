# Durable custody for personal replay conflicts

Status is proposed. LastDB currently reports the exact blocked delivery in
`cloud_sync.databases[database_id].download.blocked`. The download position
stays behind the blocked entry.

## Scope

Personal replay keeps whole-record ordering by the existing replay stamp. This
work preserves the versions involved in attachment conflicts and the exact
signed operation in schema conflicts. It leaves the device's upload sequence
independent of its download position. The cloud continues to store entries and
database-wide recovery positions.

The first implementation targets an isolated root personal database. It can
process a strictly losing attachment entry when the durable winning record is
already installed and the target has no pending own write or deferred
candidate. It preserves both versions before advancing the download position.
Overlapping attachments, an unavailable earlier winner, and incompatible
schema changes stay blocked. Shared databases and legacy log archives keep
their existing behavior.

## Why the existing state is insufficient

Replay stores at most one deferred candidate for a record. The record preimage
contains attachment markers. Applying that preimage refuses if the attachment
changed, and ordinary replacement can release the prior local file. The
accepted-write spool may have retired after cloud confirmation. A copy of
the envelope or the marker alone therefore may outlive its bytes.

The current replay transaction can write a receipt and update main state
together. Native commits publish file puts before main state and delete old
files afterward. Native file snapshots inventory both roots. These pieces
support custody once the file storage has a separate owner for each preserved
version. Current replay and recovery have no such owner or explicit custody
state.

## Format

Use a versioned main-root metadata family named `signed-replay-custody-v1`.
Each case is keyed by the database delivery sequence, encoded as a 20-digit
decimal number, and indexed by its existing replay receipt identity. The
case ID is the digest of the canonical version, scope, database ID, delivery
sequence and receipt identity. The canonical JSON case record contains:

- Version, selected cloud environment, account and generation, and database ID.
- Delivery sequence, writer key, device, writer sequence, author time,
  acceptance time, namespace, and the existing authenticated replay digest.
- The exact incoming encrypted envelope, operation kind and target. Preserve a
  schema put or drop with the installed definition identity, pending local
  operation identity when present, and the existing database order. Preserve
  the installed canonical definition and any pending operation needed to
  explain the conflict.
- The expected installed revision and winning stamp for a record, when known.
  The case holds the record preimage necessary to interpret its file markers.
  Record an absent version explicitly for a delete or clear. A preparation
  ID binds this source version and the observed pending markers to the case.
- File descriptors by role, including incoming, installed and earlier winning
  version when needed. Each records representation, byte count, ciphertext
  digest, and the authenticated content marker. Record an inline attachment
  with the same ownership and validation rules.
- State `preparing`, `held` or `resolved` and a separate disposition
  indicating whether replay remains blocked or has preserved a losing edit.
  A resolved case records the exact resolution and identifies any retained
  files.

Store each attachment as a sealed object in a dedicated native file key family
`signed-replay-custody-file-v1`, keyed by case ID, preparation ID and version
role. This identity owns the bytes independently of the current record's
file reference.
Verify the original remote object against its
authenticated binding and marker before preserving it. Verify a local copy
against the recorded native file version and marker. Keep case headers below
64 KiB. Store larger signed envelopes and definition bodies as separate
sealed objects under the same case ID. Proposed per-home defaults are 2 GiB
and 1,024 unreleased cases, including in-progress reservations. The byte
budget can hold three files at the current 512 MiB native limit with space
for case metadata. The case limit bounds metadata when cases have no files.
Each file still fits the existing file and native write limits. If a file
or the space needed to preserve it is unavailable, report the failure and
hold the download position. Exclude record values, key material, attachment
bytes and credentials from status and logs.

## Commit and replay

1. Check the owner scope and exact delivered entry. Open the authenticated
   envelope and select the blocked target. Read file data outside the native
   write gate. Acquire the gate and compare the current revision, pending
   markers and winner to the values used when preparing the case.
2. Commit a durable intent and byte reservation. Install each encrypted file
   under its exact custody key. Retry a completed put only when its stored
   length and digest match. A different identity, body or file at a reused key
   blocks recovery.
3. Under the gate, verify the complete custody inventory and the target state
   again. If the target changed, leave the position in place and reserve a
   new preparation ID. Leave the previous objects untouched until the
   abandoned intent passes the unreferenced staging check. Once verified,
   commit the held case to the main root. For a strictly losing attachment
   with an installed winner and no pending or deferred write, commit its
   ordinary replay receipt in the same transaction. Leave the winning record
   and all existing upload confirmations in place. The caller
   advances the current local download position only when the whole preceding
   prefix has durable outcomes. A crash after custody and before the cursor
   write repeats the receipt check and reuses the same file identities.
4. For incompatible schema work or uncertain attachment ordering, record the
   held case without an applied receipt. Keep the download position before the
   entry. Upload and later retry continue to use the original numbered edits.
   A restarted device can inspect or resume the held case without changing the
   order. A healthy later cloud read does not resolve it.

An independent replay lane beyond a held schema change requires a separate
design step. Before permitting it, durably record a barrier for that schema
and defer its dependent entries in delivery order. Unrelated schemas can
progress only after every examined entry has a durable outcome. A later own
echo still belongs to its original schema sequence. The first implementation
keeps the current database-wide hold for those cases.

## Retention and recovery

Provide owner-only inspection by database ID and delivery sequence. It reports
the case state, reason, versions and available files. Owner export verifies
the saved identity and digests again. Explicit resolution records which
whole-record version or schema operation was selected. A schema resolution
must account for its ordered changes and dependent records. Applying an
incompatible definition or dropping a schema requires that explicit
resolution. Enabling a shared resolution across this owner's devices
requires a durable decision in the published log and compatible readers.

Preparing or held data has no automatic expiration. A restart verifies its
reservation and file identities. A failed attempt may clean exact unreferenced
staging bytes only after proving there is no committed case or replay receipt.
An explicit owner release can retire a resolved case's file bytes after its
final disposition is durable. Keep the receipt identity and disposition so
an exact retry does not recreate the conflict or move the cursor twice.

Initially, a home with unresolved custody refuses to certify a native
recovery cutoff and refuses an automatic native reset that could replace its
custody. Direct exports and local writes remain available. Use only a DEV
database for which both automatic and manual physical reclamation stay off.
Verify that scope's reclamation capability before the operation. Support for
reclaim-enabled databases requires a database-wide capability for recovery
cuts, so older clients cannot bypass custody by certifying a newer cut.
Require the custody format version in the signed cutoff manifest for that
database and refuse unsupported versions. Enable it only after a native
snapshot can carry the complete case and files, fresh restore verifies them,
and automatic recovery proves its stage preserves all local held cases
before replacing either root. A valid prior snapshot remains usable for a
fresh explicit restore. The service needs no mutable position for a device.

## Operational verification

Extend the existing protected [DEV owner-device operation](../../scripts/mutation_log_load/private_recovery.py)
when implementing the feature. Use two fresh owner homes with distinct device
IDs and generated records in a run-owned personal database. Start from a common
cloud position. Save different files to the same record while the devices are
offline and establish the order of the author stamps. Reconnect the higher
stamp writer first and wait for its own write to settle. Reconnect the other
writer and verify that the first writer preserves the losing attachment,
advances, and still shows the winner. Its two exported custody files must
match the original file hashes. On the second writer, check the blocked
position for the overlapping pending case and verify that its upload identity
remains intact. Restart an isolated daemon during an observed preparation and
verify it completes the same case. Verify an exact replay after the receipt
is durable and before the cursor has reached it, using the product's recorded
positions to confirm that boundary. Confirm the resumed cursor and one
custody case, then read a later edit.

On a separate generated database, create incompatible definitions from the
two offline devices, followed by records that depend on those definitions.
Reconnect and confirm that the earlier blocked schema operation retains its
exact position and both definition identities while the download position
stays behind it. Restart and verify the same staged operation and no schema
replacement or loss of queued writes. Before enabling custody-aware physical
cleanup, use the existing [native snapshot operation](../../scripts/cloud_backup/snapshot_gate.py)
to restore the catalog and verify both file hashes across a physical cutoff.
Use only scoped DEV cleanup and keep the results in the protected CI summary.

## Source references

- Personal replay and blocking in [record.rs](../../lastdb_node/src/database_replay/owned/record.rs)
  and [owned.rs](../../lastdb_node/src/database_replay/owned.rs).
- Existing preimages and file ownership in [records.rs](../../fold_db/crates/core/src/fold_db_core/mutation_manager/replay/records.rs)
  and [files.rs](../../fold_db/crates/core/src/storage/format/metadata/files.rs).
- Main and file commit order in [publish.rs](../../fold_db/crates/core/src/storage/disk_pack/run_commit/publish.rs).
- Native capture and recovery checks in [disk_backup/mod.rs](../../fold_db/crates/core/src/storage/disk_backup/mod.rs).
