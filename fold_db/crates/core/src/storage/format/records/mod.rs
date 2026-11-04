//! Format-driven record encoding, using the request's existing commit owner.
//!
//! The merger supplies values and format-created addresses. This module alone
//! assembles their raw mutations, composing the existing record-file encoder.
//! The encoded batch has no public fields or raw append/unwrap operation.
//! It stages into an already-open request; it never opens or commits a home.
//!
//! History uses the sibling encoder and appends only its opaque encoded batch.
//! The provider holds read-only views; this format owns retained file/control
//! and schema-release writes too. Broad bootstrap factories remain separate.

mod alternates;
mod audit;
mod file_crypto;
mod files;
mod keys;
mod maintenance;
mod open;
mod release;
mod store;

pub(crate) use alternates::AlternateChange;
pub use alternates::AlternateEntry;
pub use audit::RecordDeleteLedgerEntry;
pub(crate) use keys::{alternate_range_key, AlternateAddress, RecordAddress};
pub(crate) use maintenance::validate_cutoff_raise;
pub(crate) use release::{release_families, ReleaseFamily, ReleaseKind};
pub(crate) use store::RecordFormat;

use crate::schema::types::FileWrite;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{record_warm::WrittenRecord, transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::history::EncodedHistory;
use crate::storage::{KvMutation, KvStore, StorageError};
use lastdb_model::record_value::{RecordValueError, StoredRecord, MAX_RECORD_BYTES};
use std::collections::HashSet;
use std::sync::Arc;

/// Prepared values of one record, after the existing mutation merge.
pub(crate) struct RecordChange {
    pub key: RecordAddress,
    /// Logical change context; never authorizes skipping file admission.
    pub before: Option<Arc<StoredRecord>>,
    pub after: StoredRecord,
    pub file_write: Option<FileWrite>,
}

/// Only this format module can create the batch or add primary/alternate rows.
struct EncodedRecords {
    main: Vec<KvMutation>,
    files: Vec<files::FileChange>,
    written: Vec<WrittenRecord>,
}

fn encode(
    changes: Vec<RecordChange>,
    alternates: Vec<AlternateChange>,
) -> Result<EncodedRecords, SchemaError> {
    let mut main = Vec::with_capacity(changes.len() + alternates.len());
    let mut files = Vec::with_capacity(changes.len());
    let mut written = Vec::with_capacity(changes.len());
    for change in changes {
        // Structure is checked now; complete-request admission checks the
        // final encoded body at each key, including repeated-key writes.
        if &change.after.key != change.key.api_key() {
            return Err(SchemaError::InvalidData(
                "record payload differs from its bound primary address".into(),
            ));
        }
        let value = change.after.encode_uncapped().map_err(record_value_error)?;
        let after = Arc::new(change.after);
        // Every changed address participates. A file-free result requires
        // its owned path to be absent, independent of the before hint.
        files.push(files::FileChange {
            key: change.key.clone(),
            after: Arc::clone(&after),
            write: change.file_write,
        });
        written.push(WrittenRecord::new(change.key.as_str(), after, &value));
        main.push(KvMutation::put(change.key.into_string(), value));
    }
    // File decisions use one current preimage per key. The coordinator
    // splits repeated keys into successive groups in the same request.
    let mut keys = HashSet::with_capacity(main.len());
    if main.iter().any(|row| !keys.insert(row.key())) {
        return Err(SchemaError::InvalidData(
            "a record batch repeats a record address".into(),
        ));
    }
    for change in alternates {
        main.push(alternates::encode(change)?);
    }
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
        mut self,
        main_store: &dyn KvStore,
        runtime: &PackRuntime,
        history: EncodedHistory,
    ) -> Result<Vec<WrittenRecord>, SchemaError> {
        if !transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "row commits require a complete native write scope".into(),
            ));
        }
        history.append_to(&mut self.main);
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
