# Last writer wins for personal attachments

> Changed on 23 November 2026. A local write now gets its stamp when it
> commits, so no local write is pending and no confirmed state is kept behind
> it. The fields, the file and the inline file of a record each take the value
> of their newest write, and a write that names no file does not change the
> file. [The home on disk](../home-format.md) describes it as built, and step 22
> of the personal database proof (`personal_proof_merge.py`) proves it on real
> nodes. The sections on a pending local write below describe the earlier rule.

Private owner-device replay orders a whole record by its accepted replay
stamp: author time capped by cloud acceptance time, then writer key, device,
and writer sequence. The same order selects external and inline attachment
changes. A losing entry receives a replay receipt without replacing the
winner or downloading its file. Ordinary app writes still require the
record condition supplied by the caller. Shared member proposals still pass
the publisher's local approval rules.

## A complete confirmed winner

Local writes remain visible while they await cloud delivery. For each record
with a pending local write, replay keeps one complete confirmed state behind
that optimistic state. It starts with the record before the first pending
write, including the known absence of a record or attachment.

An accepted entry with a greater stamp replaces the confirmed state. An
explicit attachment write replaces that slot; an explicit clear removes it.
A change that leaves an attachment alone inherits it from the confirmed
state. It does not inherit a pending local attachment. External and inline
files are independent slots, so a photo can keep both its full image and its
thumbnail.

The latest local write's echo settles the pending state. If that echo wins,
it is folded into the confirmed state and the result becomes visible. If it
loses, the already confirmed winner becomes visible. The receipt, record,
attachment state, and release of temporary holds commit together.

For example, one device writes photo A at time 10, then changes its caption
at time 30 without supplying a photo. Another device writes photo B at time
20. When these arrive in that order, the final caption keeps photo B on every
device. Leaving the first device's optimistic photo A in place when its
caption echo wins would make the devices disagree.

## File lifetime

A pending record's existing external file version stays available while the
confirmed state names it. Inline bytes have a separate bounded saved row.
A winning remote file is fetched, decrypted, and verified before replay
commits its candidate and receipt. Its bytes are then held locally, so later
settlement does not depend on the cloud retaining an already acknowledged
object. A later caption-only entry can keep that file without losing it.

Superseding the confirmed state releases an external version only when the
live record no longer names it. Settlement and schema deletion release the
remaining temporary holds. These are temporary files needed to install the
winner, not a permanent archive of losing conflicts. The record and inline
size limits remain unchanged; large file bytes do not enter record metadata
or the warm set.

## Compatibility and failures

Existing replay receipts and ordering stamps remain valid. Older deferred
text entries can be converted when their complete earlier attachment state
is still available. A home that already deleted the required earlier file
cannot recover it from a marker alone. Replay stops instead of combining the
winner's fields with a losing attachment. Missing or damaged required bytes
and incompatible schemas remain real replay failures.

The download cursor advances only after replay has durably handled its
preceding entries. A retry after a committed receipt skips that entry. A
failed native transaction keeps the pending state and does not acknowledge
the failed change. Native snapshots retain their existing settled-replay
requirements. This change does not upgrade a running primary or change cloud
retention policy.

## Source

- [Owner replay](../../lastdb_node/src/database_replay/owned.rs) prepares files
  outside the write transaction and retries against current replay state.
- [Replay application](../../lastdb_node/src/database_replay/apply.rs) chooses
  the winner and settles pending local writes.
- [Record replay](../../fold_db/crates/core/src/fold_db_core/mutation_manager/replay/candidate.rs)
  owns the confirmed record and its materialization.
- [Pending state](../../fold_db/crates/core/src/fold_db_core/mutation_manager/replay/state.rs)
  captures the first preimage and releases temporary holds.
- [Native record storage](../../fold_db/crates/core/src/storage/format/records.rs)
  commits record and file changes through the normal storage path.
