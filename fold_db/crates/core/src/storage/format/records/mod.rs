//! Format-driven record encoding, using the request's existing commit owner.
//!
//! The mutation provider supplies values and format-created addresses. This module
//! derives alternate pointer changes and composes the record-file encoder.
//! The encoded batch has no public fields or raw append/unwrap operation.
//! It stages into an already-open request; it never opens or commits a home.
//!
//! The provider holds read-only views; this format owns retained file/control
//! and schema-release writes too. Broad bootstrap factories remain separate.

mod alternates;
pub(in crate::storage) mod applied_deletes;
mod audit;
mod file_crypto;
mod files;
mod keys;
mod maintenance;
mod open;
mod outgoing;
mod release;
mod retention;
mod store;

pub use alternates::{AlternateEntry, MAX_ALTERNATE_ENTRIES};
pub(crate) use applied_deletes::ReclaimedDeletes;
pub use audit::RecordDeleteLedgerEntry;
pub(crate) use keys::{alternate_range_key, AlternateAddress, RecordAddress};
pub(crate) use maintenance::validate_cutoff_raise;
pub(crate) use release::{release_families, ReleaseFamily, ReleaseKind};
pub(crate) use store::RecordFormat;

use crate::schema::types::FileWrite;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{record_warm::WrittenRecord, transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, KvStore, StorageError};
use lastdb_model::record_value::{RecordValueError, StoredRecord, MAX_RECORD_BYTES};
use std::collections::HashSet;
use std::sync::Arc;

/// Prepared values of one complete replacement or erasure.
pub(crate) struct RecordChange {
    pub key: RecordAddress,
    /// Logical change context; never authorizes skipping file admission.
    pub before: Option<Arc<StoredRecord>>,
    pub after: StoredRecord,
    pub file_write: Option<FileWrite>,
    pub portable: PortableEffect,
}

pub(crate) enum PortableEffect {
    Local,
    SharedAuthor(String),
    SharedReplay,
}

/// Only this format module can create the batch or add primary/alternate rows.
struct EncodedRecords {
    main: Vec<KvMutation>,
    files: Vec<files::FileChange>,
    written: Vec<WrittenRecord>,
}

fn encode(
    changes: Vec<RecordChange>,
    alternates: Vec<KvMutation>,
) -> Result<EncodedRecords, SchemaError> {
    let mut main = Vec::with_capacity(changes.len() + alternates.len());
    let mut files = Vec::with_capacity(changes.len());
    let mut written = Vec::with_capacity(changes.len());
    let mut keys = HashSet::with_capacity(changes.len());
    for change in &changes {
        if !keys.insert(change.key.as_str()) {
            return Err(SchemaError::InvalidData(
                "a record batch repeats a record address".into(),
            ));
        }
    }
    for change in changes {
        // Structure is checked now; complete-request admission checks the
        // final encoded body at each key, including repeated-key writes.
        if change.after.is_live() && &change.after.key != change.key.api_key() {
            return Err(SchemaError::InvalidData(
                "record payload differs from its bound primary address".into(),
            ));
        }
        let after = Arc::new(change.after);
        // Every changed address participates in the file-version decision.
        files.push(files::FileChange {
            key: change.key.clone(),
            after: Arc::clone(&after),
            write: change.file_write,
            shared: !matches!(change.portable, PortableEffect::Local),
            needs_upload: matches!(change.portable, PortableEffect::SharedAuthor(_)),
        });
        if after.is_live() {
            let value = after.encode_uncapped().map_err(record_value_error)?;
            written.push(WrittenRecord::new(change.key.as_str(), after, &value));
            main.push(KvMutation::put(change.key.into_string(), value));
        } else {
            main.push(applied_deletes::encode(&change.key, &after.stamp)?);
            if !matches!(change.portable, PortableEffect::Local) {
                main.push(applied_deletes::encode_age(&change.key, &after.stamp)?);
            }
            main.push(KvMutation::delete(change.key.into_string()));
        }
    }
    main.extend(alternates);
    Ok(EncodedRecords {
        main,
        files,
        written,
    })
}

impl EncodedRecords {
    /// Stage only into the coordinator's current request. No child writer
    /// gets a namespace factory or starts an independent transaction.
    async fn stage(
        self,
        main_store: &dyn KvStore,
        runtime: &PackRuntime,
    ) -> Result<Vec<WrittenRecord>, SchemaError> {
        if !transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "row commits require a complete native write scope".into(),
            ));
        }
        // Keep the original single staging call and its native admission
        // bound, including no-op deletes. The complete request owns commit.
        main_store.batch_mutate(self.main).await?;
        Ok(self.written)
    }
}

/// Native record addresses are canonical UTF-8 keys stored verbatim. Check
/// only final staged bodies, not intermediate writes or any stored-key census.
pub(in crate::storage) fn require_final_record_sizes<'a>(
    max_body_bytes: usize,
    records: impl Iterator<Item = (&'a str, &'a str, usize)>,
) -> StorageResult<()> {
    // The native accounting loop derives this upper bound from every staged
    // body. If all bodies fit, every final record necessarily fits too, so
    // the lazy iterator need not visit any keys in the ordinary case.
    if max_body_bytes <= MAX_RECORD_BYTES {
        return Ok(());
    }
    for (collection, key, bytes) in records {
        if collection == super::RECORDS_COLLECTION
            && key.starts_with(lastdb_model::record_space::RECORD_PREFIX)
            && bytes > MAX_RECORD_BYTES
        {
            return Err(StorageError::RecordTooLarge {
                size: bytes,
                limit: MAX_RECORD_BYTES,
            });
        }
    }
    Ok(())
}

fn record_value_error(error: RecordValueError) -> SchemaError {
    match error {
        RecordValueError::TooLarge { bytes } => SchemaError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        },
        other => SchemaError::InvalidData(format!("the record write is refused: {other}")),
    }
}
