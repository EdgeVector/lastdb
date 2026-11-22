//! Upsert or delete each LWW winner of a `VerifiedHistory`.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::{CasExpectation, FileWrite, KeyValue, Mutation, MutationType};
use mutation_log::{MaterializedRecord, VerifiedHistory};
use serde_json::Value;

use crate::files::FileSource;
use crate::key::key_value;
use crate::read::read_at;
use crate::value::{Alternates, FileRef, RecordValue};

pub(crate) struct Stored {
    pub(crate) revision: String,
    pub(crate) created_by: Option<String>,
    pub(crate) updated_by: Option<String>,
    pub(crate) fields: HashMap<String, Value>,
    pub(crate) alternates: Alternates,
    pub(crate) file: Option<FileMarker>,
}

/// Make FoldDB hold exactly the winners of `history`.
///
/// A winner already stored with equal fields, alternate addresses and file, and
/// the same authors, is left alone. History records are whole-record
/// last-writer-wins, so the winner's writer is both `created_by` and
/// `updated_by` on every node. An empty value deletes the record, and the
/// delete names the record's stored alternate addresses. A winner that names a
/// file puts that file into the store, with the bytes that `files` gives. A
/// live winner whose namespace has no declared schema fails closed. A tombstone
/// for an undeclared namespace is skipped so a tombstone-only namespace does
/// not block restore.
pub async fn apply_history(
    store: &FoldDB,
    history: &VerifiedHistory,
    files: &dyn FileSource,
) -> Result<(), String> {
    for ((namespace, record_key), winner) in history.records() {
        apply_winner(store, namespace, record_key, winner, files).await?;
    }
    Ok(())
}

async fn apply_winner(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    winner: &MaterializedRecord,
    files: &dyn FileSource,
) -> Result<(), String> {
    if !schema_declared(store, namespace)? {
        if winner.value.is_empty() {
            return Ok(());
        }
        return Err(format!(
            "schema {namespace} has no declaration in signed history"
        ));
    }
    let key = key_value(store, namespace, record_key)?;
    let current = read_at(store, namespace, &key).await?;
    let wanted = decode_winner(&winner.value)?;
    let author = winner.author.to_hex();
    let (kind, expected, alternates) = match (&current, &wanted) {
        (None, None) => return Ok(()),
        (None, Some(new)) => (
            MutationType::Create,
            CasExpectation::Absent,
            new.alternates.clone(),
        ),
        (Some(old), Some(new)) if unchanged(old, new, &author) => return Ok(()),
        (Some(old), Some(new)) => (
            MutationType::Replace,
            revision_of(old),
            new.alternates.clone(),
        ),
        (Some(old), None) => (
            MutationType::Delete,
            revision_of(old),
            old.alternates.clone(),
        ),
    };
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates,
    };
    let fields: HashMap<String, Value> = wanted
        .as_ref()
        .map(|new| new.fields.clone().into_iter().collect())
        .unwrap_or_default();
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        kind,
        expected,
    );
    mutation.writer = Some(author.clone());
    if let Some(new) = &wanted {
        set_file(&mut mutation, store, files, current.as_ref(), new)?;
        mutation.copied_authorship = Some((Some(author.clone()), Some(author)));
    }
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| format!("write {namespace}/{record_key}: {e}"))
}

fn decode_winner(value: &[u8]) -> Result<Option<RecordValue>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    RecordValue::decode(value).map(Some)
}

/// True when the stored record already is the winner: the same fields,
/// alternate addresses and file, written by the same author.
fn unchanged(old: &Stored, new: &RecordValue, author: &str) -> bool {
    old.fields.len() == new.fields.len()
        && new
            .fields
            .iter()
            .all(|(name, value)| old.fields.get(name) == Some(value))
        && old.alternates == new.alternates
        && file_matches(old.file.as_ref(), new.file.as_ref())
        && old.created_by.as_deref() == Some(author)
        && old.updated_by.as_deref() == Some(author)
}

fn file_matches(stored: Option<&FileMarker>, wanted: Option<&FileRef>) -> bool {
    match (stored, wanted) {
        (None, None) => true,
        (Some(marker), Some(file)) => {
            marker.sha256 == file.sha256.to_hex()
                && marker.size_bytes == file.size_bytes
                && marker.name == file.name
                && marker.media_type == file.media_type
        }
        _ => false,
    }
}

/// Put the winner's file into the store, or clear the record's file when the
/// winner holds none. A file the store already holds stays as it is.
fn set_file(
    mutation: &mut Mutation,
    store: &FoldDB,
    files: &dyn FileSource,
    current: Option<&Stored>,
    new: &RecordValue,
) -> Result<(), String> {
    let stored = current.and_then(|old| old.file.as_ref());
    let Some(file) = &new.file else {
        if stored.is_some() {
            mutation.file_write = Some(FileWrite::Clear);
        }
        return Ok(());
    };
    if file_matches(stored, Some(file)) {
        return Ok(());
    }
    let bytes = files.open(file)?;
    let write = store
        .db_ops()
        .records()
        .seal_record_file(&bytes, file.name.clone(), file.media_type.clone())
        .map_err(|e| format!("keep the file of the record: {e}"))?;
    let FileWrite::Put(sealed) = &write else {
        return Err("the store did not seal the file of the record".into());
    };
    let marker = sealed.marker().clone();
    if marker.sha256 != file.sha256.to_hex() || marker.size_bytes != file.size_bytes {
        return Err("the file bytes differ from the file the record names".into());
    }
    mutation.file = Some(marker);
    mutation.file_write = Some(write);
    Ok(())
}

fn revision_of(stored: &Stored) -> CasExpectation {
    CasExpectation::Revision {
        revision: stored.revision.clone(),
    }
}

fn schema_declared(store: &FoldDB, namespace: &str) -> Result<bool, String> {
    Ok(store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
        .is_some())
}

/// Make the store hold what the history holds at each of `keys`: the winner's
/// record, or no record when no write counts there.
///
/// This is the one way a store is brought back to its history at given keys. It
/// undoes a write whose signed entry could not be signed. A key of an
/// undeclared schema is skipped.
pub async fn reconcile_keys<'a>(
    store: &FoldDB,
    history: &VerifiedHistory,
    keys: impl IntoIterator<Item = &'a (String, String)>,
    files: &dyn FileSource,
) -> Result<(), String> {
    for key in keys {
        let (namespace, record_key) = key;
        if !schema_declared(store, namespace)? {
            continue;
        }
        if let Some(winner) = history.records().get(key) {
            apply_winner(store, namespace, record_key, winner, files).await?;
        } else {
            let at = key_value(store, namespace, record_key)?;
            delete_record(store, namespace, at).await?;
        }
    }
    Ok(())
}

async fn delete_record(store: &FoldDB, namespace: &str, key: KeyValue) -> Result<(), String> {
    let Some(current) = read_at(store, namespace, &key).await? else {
        return Ok(());
    };
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates: current.alternates.clone(),
    };
    let mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        HashMap::new(),
        MutationType::Delete,
        revision_of(&current),
    );
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| format!("delete {namespace}: {e}"))
}
