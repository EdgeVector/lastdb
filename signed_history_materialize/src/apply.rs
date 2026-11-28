//! Upsert or delete each LWW winner of a `VerifiedHistory`.

use std::collections::HashMap;

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::{CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Schema};
use mutation_log::{MaterializedRecord, VerifiedHistory};
use serde_json::Value;

use crate::attachments::FileSection;
use crate::key::key_value_for_schema;
use crate::read::read_at;
use crate::validate::validate_record;
use crate::value::{decode_record_value, resolve_schema_alternates, DecodedRecord, Stored};

/// Make FoldDB hold the valid winners of `history`.
///
/// Winners are applied in deterministic descending `RecordPosition` order. A
/// winner already stored with equal fields, equal alternates, and the same
/// `created_by` and `updated_by` is left alone. An empty value deletes the
/// record. A malformed or schema-invalid winner logs a warning and removes any
/// previously materialized record at that key so every node converges on the
/// same state (record absent), while storage/CAS errors always propagate.
pub async fn apply_history(store: &FoldDB, history: &VerifiedHistory) -> Result<(), String> {
    let mut winners: Vec<_> = history.records().iter().collect();
    winners.sort_by(|(key_a, rec_a), (key_b, rec_b)| {
        let pos_a = (
            rec_a.position.lamport,
            rec_a.position.node_key,
            rec_a.position.device_id,
        );
        let pos_b = (
            rec_b.position.lamport,
            rec_b.position.node_key,
            rec_b.position.device_id,
        );
        pos_b.cmp(&pos_a).then_with(|| key_a.cmp(key_b))
    });
    for ((namespace, record_key), winner) in winners {
        apply_record_winner(store, namespace, record_key, winner).await?;
    }
    Ok(())
}

/// Apply one winner when no external file Put needs a fetched blob.
pub async fn apply_record_winner(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    winner: &MaterializedRecord,
) -> Result<(), String> {
    apply_winner(store, namespace, record_key, winner).await
}

async fn apply_winner(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    winner: &MaterializedRecord,
) -> Result<(), String> {
    let Some(schema) = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
    else {
        if !winner.value.is_empty() {
            tracing::warn!(
                namespace = %namespace,
                "skipping record winner whose schema has no declaration in signed history"
            );
        }
        return Ok(());
    };
    let key = match key_value_for_schema(&schema, namespace, record_key) {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(
                namespace = %namespace,
                %error,
                "skipping record winner with invalid primary key encoding"
            );
            return Ok(());
        }
    };
    let current = read_at(store, namespace, &key).await?;
    let Some(mutation) = plan_winner(&schema, namespace, &key, current.as_ref(), winner, None)
    else {
        return Ok(());
    };
    write_one(store, namespace, record_key, mutation).await
}

/// Apply one winner when an external file Put has already been sealed locally.
pub async fn apply_winner_with_file(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    winner: &MaterializedRecord,
    external: FileWrite,
) -> Result<(), String> {
    let Some(schema) = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
    else {
        return Ok(());
    };
    let key = key_value_for_schema(&schema, namespace, record_key)?;
    let current = read_at(store, namespace, &key).await?;
    let Some(mutation) = plan_winner(
        &schema,
        namespace,
        &key,
        current.as_ref(),
        winner,
        Some(external),
    ) else {
        return Ok(());
    };
    write_one(store, namespace, record_key, mutation).await
}

async fn write_one(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    mutation: Mutation,
) -> Result<(), String> {
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|error| format!("write {namespace}/{record_key}: {error}"))
}

fn plan_winner(
    schema: &Schema,
    namespace: &str,
    key: &KeyValue,
    current: Option<&Stored>,
    winner: &MaterializedRecord,
    external: Option<FileWrite>,
) -> Option<Mutation> {
    if winner.value.is_empty() {
        return current.map(|old| delete_mutation(namespace, key.clone(), old));
    }
    let creator = winner.creator.to_hex();
    let author = winner.author.to_hex();
    let (target, fields, decoded) = match decode_and_validate(schema, namespace, key, &winner.value)
    {
        Ok(valid) => valid,
        Err(reason) => {
            tracing::warn!(
                namespace = %namespace,
                %reason,
                "dropping invalid record winner in signed history"
            );
            return current.map(|old| delete_mutation(namespace, key.clone(), old));
        }
    };
    let (kind, expected) = match current {
        None => (MutationType::Create, CasExpectation::Absent),
        Some(old) => {
            if old.fields == fields
                && old.alternates == target.alternates
                && old.created_by.as_deref() == Some(creator.as_str())
                && old.updated_by.as_deref() == Some(author.as_str())
                && attachment_matches(old, &decoded.file, decoded.inline.as_ref())
            {
                return None;
            }
            (MutationType::Replace, revision_of(old))
        }
    };
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        kind,
        expected,
    );
    mutation.writer = Some(author.clone());
    mutation.copied_authorship = Some((Some(creator), Some(author)));
    if let FileSection::Put(marker) = &decoded.file {
        let Some(write) = external else {
            // Needs a fetched blob; rematerialize calls [`apply_winner_with_file`].
            return None;
        };
        prepare_external_file(&mut mutation, marker, write).ok()?;
        mutation.inline_write = decoded.inline;
        return Some(mutation);
    }
    apply_decoded_attachments(&mut mutation, &decoded.file, decoded.inline).ok()?;
    Some(mutation)
}

fn attachment_matches(old: &Stored, file: &FileSection, inline: Option<&InlineWrite>) -> bool {
    let file_ok = match file {
        FileSection::Keep => true,
        FileSection::Clear => old.file.is_none(),
        FileSection::Put(marker) => old.file.as_ref() == Some(marker),
    };
    let inline_ok = match inline {
        None => true,
        Some(InlineWrite::Clear) => old.inline_file.is_none(),
        Some(InlineWrite::Put(file)) => old.inline_file.as_ref() == Some(&file.marker),
    };
    file_ok && inline_ok
}

/// Apply attachment sections that need no blob fetch. An external Put is left
/// for [`prepare_external_file`] after the sealed blob is opened.
fn apply_decoded_attachments(
    mutation: &mut Mutation,
    file: &FileSection,
    inline: Option<InlineWrite>,
) -> Result<(), String> {
    match file {
        FileSection::Keep => {}
        FileSection::Clear => mutation.file_write = Some(fold_db::schema::types::FileWrite::Clear),
        FileSection::Put(marker) => {
            mutation.file = Some(marker.clone());
            return Err("record file blob has not been prepared".into());
        }
    }
    mutation.inline_write = inline;
    Ok(())
}

/// Attach a sealed store file write prepared from a fetched relay blob.
pub fn prepare_external_file(
    mutation: &mut Mutation,
    marker: &fold_db::record_value::FileMarker,
    write: fold_db::schema::types::FileWrite,
) -> Result<(), String> {
    let matches = match &write {
        fold_db::schema::types::FileWrite::Put(sealed) => sealed.marker() == marker,
        fold_db::schema::types::FileWrite::LocalPath(local) => local.marker() == marker,
        fold_db::schema::types::FileWrite::Clear => false,
    };
    if !matches {
        return Err("prepared file write does not match the history marker".into());
    }
    mutation.file = Some(marker.clone());
    mutation.file_write = Some(write);
    Ok(())
}

fn decode_and_validate(
    schema: &Schema,
    namespace: &str,
    key: &KeyValue,
    raw_value: &[u8],
) -> Result<(RecordTarget, HashMap<String, Value>, DecodedRecord), String> {
    let rec = decode_record_value(raw_value)?
        .ok_or_else(|| "live record winner unexpectedly decoded as empty".to_owned())?;
    let alternates = resolve_schema_alternates(schema, rec.alternates.clone())?;
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key: key.clone(),
        alternates,
    };
    let file_marker = match &rec.file {
        FileSection::Put(marker) => Some(marker),
        FileSection::Keep | FileSection::Clear => None,
    };
    let inline_marker = match &rec.inline {
        Some(InlineWrite::Put(file)) => Some(&file.marker),
        Some(InlineWrite::Clear) | None => None,
    };
    validate_record(schema, &target, &rec.fields, file_marker, inline_marker)?;
    Ok((target, rec.fields.clone(), rec))
}

fn delete_mutation(namespace: &str, key: KeyValue, stored: &Stored) -> Mutation {
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates: stored.alternates.clone(),
    };
    Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        HashMap::new(),
        MutationType::Delete,
        revision_of(stored),
    )
}

fn revision_of(stored: &Stored) -> CasExpectation {
    CasExpectation::Revision {
        revision: stored.revision.clone(),
    }
}

/// Make the store hold what the history holds at each of `keys`: the winner's
/// record, or no record when no write counts there.
pub async fn reconcile_keys<'a>(
    store: &FoldDB,
    history: &VerifiedHistory,
    keys: impl IntoIterator<Item = &'a (String, String)>,
) -> Result<(), String> {
    for key in keys {
        let (namespace, record_key) = key;
        let Some(schema) = store
            .schema_manager()
            .get_schema_metadata(namespace)
            .map_err(|e| e.to_string())?
        else {
            continue;
        };
        if let Some(winner) = history.records().get(key) {
            apply_winner(store, namespace, record_key, winner).await?;
        } else {
            let Ok(at) = key_value_for_schema(&schema, namespace, record_key) else {
                continue;
            };
            delete_record(store, namespace, at).await?;
        }
    }
    Ok(())
}

async fn delete_record(store: &FoldDB, namespace: &str, key: KeyValue) -> Result<(), String> {
    let Some(current) = read_at(store, namespace, &key).await? else {
        return Ok(());
    };
    delete_stored(store, namespace, key, &current).await
}

async fn delete_stored(
    store: &FoldDB,
    namespace: &str,
    key: KeyValue,
    current: &Stored,
) -> Result<(), String> {
    let mutation = delete_mutation(namespace, key, current);
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| format!("delete {namespace}: {e}"))
}
