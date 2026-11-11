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
//! The missing call is one public prefix for one data directory. Each item
//! must carry the file, the file write, the inline write, the canonical
//! schema definition, and the contribution, already sealed by that
//! database's content key. Replay is idempotent on the mutation id. E3 owns
//! that envelope. This module does not canonicalize record fields and does
//! not invent a cipher.
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
