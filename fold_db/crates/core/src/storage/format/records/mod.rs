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
pub(crate) use keys::{AlternateAddress, RecordAddress};
pub(crate) use maintenance::validate_cutoff_raise;
pub(crate) use release::{release_families, ReleaseFamily, ReleaseKind};
pub(crate) use store::RecordFormat;

use crate::schema::SchemaError;
use crate::storage::disk_pack::{record_warm::WrittenRecord, transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::history::EncodedHistory;
use crate::storage::{KvMutation, KvStore, StorageError};
use lastdb_model::record_value::{RecordValueError, StoredRecord, MAX_RECORD_BYTES};
use std::sync::Arc;

/// Prepared values of one record, after the existing mutation merge.
pub(crate) struct RecordChange {
    pub key: RecordAddress,
    pub before: Option<Arc<StoredRecord>>,
    pub after: StoredRecord,
    pub file_row: Option<Arc<Vec<u8>>>,
}

/// Only this format module can create the batch or add primary/alternate rows.
struct EncodedRecords {
    main: Vec<KvMutation>,
    effects: RecordEffects,
}

/// Private file changes and memory publications consumed by the bound writer.
/// Neither the raw main batch nor these effects escape to a provider.
struct RecordEffects {
    files: Vec<KvMutation>,
    written: Vec<WrittenRecord>,
}

fn encode(
    changes: Vec<RecordChange>,
    alternates: Vec<AlternateChange>,
    content_key: Option<&[u8; 32]>,
) -> Result<EncodedRecords, SchemaError> {
    let mut main = Vec::with_capacity(changes.len() + alternates.len());
    let mut files = Vec::new();
    let mut written = Vec::with_capacity(changes.len());
    for mut change in changes {
        files.extend(files::encode(
            &change.key,
            change.before.as_deref(),
            &change.after,
            change.file_row.take(),
            content_key,
        )?);
        // Structure is checked now; complete-request admission checks the
        // final encoded body at each key, including repeated-key writes.
        let value = change.after.encode_uncapped().map_err(record_value_error)?;
        written.push(WrittenRecord::new(
            change.key.as_str(),
            Arc::new(change.after),
            &value,
        ));
        main.push(KvMutation::put(change.key.into_string(), value));
    }
    for change in alternates {
        main.push(alternates::encode(change)?);
    }
    Ok(EncodedRecords {
        main,
        effects: RecordEffects { files, written },
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
    ) -> Result<RecordEffects, SchemaError> {
        if !transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "row commits require a complete native write scope".into(),
            ));
        }
        history.append_to(&mut self.main);
        // Keep the original single staging call and its native admission
        // bound, including no-op deletes. The complete request owns commit.
        main_store.batch_mutate(self.main).await?;
        Ok(self.effects)
    }
}

/// Native record addresses are canonical UTF-8 keys stored verbatim. Check
/// only final staged bodies, not intermediate writes or any stored-key census.
pub(in crate::storage) fn require_final_record_size(
    collection: &str,
    key: &str,
    bytes: usize,
) -> StorageResult<()> {
    if collection == super::RECORDS_COLLECTION
        && key.starts_with(lastdb_model::record_space::RECORD_PREFIX)
        && bytes > MAX_RECORD_BYTES
    {
        return Err(StorageError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        });
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
