//! The values that a write call puts into the signed log.
//!
//! A value is what the store holds for the record after the commit: its fields,
//! its address under every alternate key, and its file (see `RecordValue`).
//! Reading it back, and not copying the request, is what makes the record on
//! another device the same record: the store has checked the fields and made
//! the alternate addresses canonical, and a replace that omitted its file still
//! holds the file it kept. A record the store no longer holds is a tombstone.

use fold_db::fold_db_core::FoldDB;
use mutation_log::Hex32;
use signed_history_materialize::{read_state, RecordValue};

use super::files_out::FileKeeper;
use super::mirror::OwnedMutation;

/// One record of a write call, ready to be signed.
pub(super) struct Prepared {
    pub(super) namespace: String,
    pub(super) record_key: String,
    /// The value of the history record. Empty is a tombstone.
    pub(super) value: Vec<u8>,
    /// The sealed blob of the record's file, when it has one.
    pub(super) blob: Option<Hex32>,
}

/// Read each record of `items` back from the store. A `keeper` seals and keeps
/// the files of the records; without one the records carry no file.
pub(super) async fn prepare(
    store: &FoldDB,
    items: &[OwnedMutation],
    keeper: Option<&FileKeeper<'_>>,
) -> Result<Vec<Prepared>, String> {
    let mut prepared = Vec::with_capacity(items.len());
    for item in items {
        let state = read_state(store, &item.schema, &item.record_key).await?;
        let (value, blob) = match state {
            None => (Vec::new(), None),
            Some(state) => {
                let file = match (&state.file, keeper) {
                    (Some(marker), Some(keeper)) => Some(
                        keeper
                            .file_ref(store, &item.schema, &item.record_key, marker)
                            .await?,
                    ),
                    _ => None,
                };
                let blob = file.as_ref().map(|file| file.blob);
                let record = RecordValue {
                    fields: state.fields.into_iter().collect(),
                    alternates: state.alternates,
                    file,
                };
                (record.encode(), blob)
            }
        };
        prepared.push(Prepared {
            namespace: item.schema.clone(),
            record_key: item.record_key.clone(),
            value,
            blob,
        });
    }
    Ok(prepared)
}
