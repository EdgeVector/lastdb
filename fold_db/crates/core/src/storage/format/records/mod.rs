//! Format-driven record encoding, using the request's existing commit owner.
//!
//! The merger supplies values and format-created addresses. This module alone
//! assembles their raw mutations, composing the existing record-file encoder.
//! The encoded batch has no public fields or raw append/unwrap operation.
//! It stages into an already-open request; it never opens or commits a home.
//!
//! History and other retained raw writers have not migrated yet. They stage
//! separately in that same request and are outside this partial boundary.

mod alternates;
mod files;
mod keys;

pub(crate) use alternates::AlternateChange;
pub use alternates::AlternateEntry;
pub(crate) use keys::{AlternateAddress, RecordAddress};

use crate::schema::SchemaError;
use crate::storage::disk_pack::{record_warm::WrittenRecord, transaction, PackRuntime};
use crate::storage::{KvMutation, KvStore, StorageError};
use lastdb_model::record_value::{RecordValueError, StoredRecord, MAX_RECORD_BYTES};
use std::sync::Arc;

/// Prepared values of one record, after the existing mutation merge.
pub(crate) struct RecordChange {
    pub key: RecordAddress,
    pub before: Option<Arc<StoredRecord>>,
    pub after: StoredRecord,
    /// Earlier groups of a repeated key may exceed the cap temporarily. The
    /// request still checks its final stored state before committing.
    pub cap_deferred: bool,
    pub file_row: Option<Arc<Vec<u8>>>,
}

/// Only this format module can create the batch or add primary/alternate rows.
pub(crate) struct EncodedRecords {
    main: Vec<KvMutation>,
    effects: RecordEffects,
}

/// Request-private file changes and memory publications, consumed by the
/// existing coordinator. These do not contain the primary/alternate batch.
pub(crate) struct RecordEffects {
    pub files: Vec<KvMutation>,
    pub written: Vec<WrittenRecord>,
}

pub(crate) fn encode(
    changes: Vec<RecordChange>,
    alternates: Vec<AlternateChange>,
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
        )?);
        let value = if change.cap_deferred {
            change.after.encode_uncapped()
        } else {
            change.after.encode()
        }
        .map_err(record_value_error)?;
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
    pub(crate) async fn stage(
        self,
        main_store: &dyn KvStore,
        runtime: &PackRuntime,
        following_history_rows: usize,
    ) -> Result<RecordEffects, SchemaError> {
        if !transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "row commits require a complete native write scope".into(),
            ));
        }
        // Preserve the old combined group's admission bound even though its
        // legacy history now stages separately. No-op deletes count here too.
        let operation_limit = runtime.batch_operation_limit();
        if self.main.len().saturating_add(following_history_rows) > operation_limit {
            return Err(StorageError::InvalidOperation(format!(
                "native mutation exceeds {operation_limit} operation admission limit"
            ))
            .into());
        }
        main_store.batch_mutate(self.main).await?;
        Ok(self.effects)
    }
}

pub(crate) fn require_record_fits(record: &StoredRecord) -> Result<(), SchemaError> {
    record.encode().map(|_| ()).map_err(record_value_error)
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
