//! Blocked read of the durable accepted-mutation prefix.
//!
//! A create or write is durable inside the native commit, before the caller
//! is acknowledged. `PackRuntime::commit_run` opens `MutationLog::begin`
//! (`Recording`, visible only inside `fold_db` storage) and retains the
//! native run for that data directory. The owner directory is `<home>/data`.
//! A sibling directory is `<home>/databases/<id>/data`.
//!
//! That journal is not a named-log append. `prepare_mutation_log_capture`
//! freezes it only for a byte-backup `Begin` whose destinations are R2 and
//! B2, then `MutationLogCapture::reader` yields encrypted `.ldrun` bytes.
//! `begin` returns nothing when the journal is off, gapped, or past the
//! frontier, and `finish` keeps the native commit when its marker fails.
//! Those writes are absent from the journal.
//!
//! The reader this daemon must call is `lastdb_node::accepted_writes`:
//! `prefix`, `acknowledge`, `note_snapshot`, and `status`. That module is not
//! in this tree. [`prefix`] below stays [`Blocked`]. A blocked page is not
//! published and is not acknowledged. This module does not copy an untracked
//! reader, does not seal a side path, and does not invent a cipher.
//! `log_envelope` seals a `Publish` canonical body only after `prefix`
//! returns one.
//!
//! An empty client does not receive the original database id by an edit of
//! `registry.json`. A share uses `registered_share::accept` (`lastdb share
//! accept`), which keeps the invitation stream id. An owner restore uses
//! `byte_cloud_restore::session::RestoreSession::restore`. E2 installs a
//! snapshot only after one of those bootstraps has opened that same id.
//! This module does not write the registry and does not install a snapshot.

use std::path::Path;

/// Named sync cannot see this database's accepted prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Blocked;

impl std::fmt::Display for Blocked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("accepted mutation prefix has no public reader for this data directory")
    }
}

/// The native commit remains the durable capture. There is no side queue.
pub(crate) fn prefix(_data_dir: &Path, _database_id: &str) -> Result<(), Blocked> {
    Err(Blocked)
}
