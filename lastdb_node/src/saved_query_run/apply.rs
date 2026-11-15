//! Copy and remove destination records. A plain mutation is used only when
//! no file copy can carry the fields. File clears happen before a delete.

use std::collections::BTreeMap;

use crate::saved_query::{FileMark, LiveRow, RecordKey, Remove, Step, Write};
use base64::Engine as _;
use serde_json::{json, Map, Value};

use super::store::TargetStore;

pub(super) async fn apply(
    source: &TargetStore<'_>,
    destination: &TargetStore<'_>,
    step: &Step,
) -> Result<(), String> {
    match step {
        Step::Write(write) => apply_write(source, destination, write).await,
        Step::Remove(remove) => apply_remove(destination, remove).await,
    }
}

pub(super) async fn read_live(
    destination: &TargetStore<'_>,
    keys: &[RecordKey],
) -> Result<BTreeMap<RecordKey, LiveRow>, String> {
    let mut grouped: BTreeMap<(String, String), Vec<&RecordKey>> = BTreeMap::new();
    for key in keys {
        grouped
            .entry((key.namespace.clone(), key.schema.clone()))
            .or_default()
            .push(key);
    }
    let mut live = BTreeMap::new();
    for ((namespace, schema), keys) in grouped {
        for chunk in keys.chunks(crate::saved_query::MAX_RECORDS) {
            read_chunk(destination, &namespace, &schema, chunk, &mut live).await?;
        }
    }
    Ok(live)
}

async fn read_chunk(
    destination: &TargetStore<'_>,
    namespace: &str,
    schema: &str,
    keys: &[&RecordKey],
    live: &mut BTreeMap<RecordKey, LiveRow>,
) -> Result<(), String> {
    let pairs: Vec<(&str, &str)> = keys
        .iter()
        .map(|key| (key.hash.as_str(), key.range.as_deref().unwrap_or("")))
        .collect();
    let qualified = format!("{namespace}/{schema}");
    let rows = destination
        .query(&qualified, &json!({"HashRangeKeys": pairs}))
        .await?;
    for row in rows {
        let parsed = LiveRow::from_query(namespace, schema, &row)
            .map_err(|error| error.as_str().to_owned())?;
        live.insert(parsed.key.clone(), parsed);
    }
    Ok(())
}

async fn apply_write(
    source: &TargetStore<'_>,
    destination: &TargetStore<'_>,
    write: &Write,
) -> Result<(), String> {
    let mut revision = write.revision.clone();
    let carries = write.copy_blob.is_some() || write.copy_inline.is_some();
    if (write.creating || write.fields_changed) && !carries {
        let operation = if write.creating { "create" } else { "replace" };
        revision = Some(
            mutate(
                destination,
                &write.key,
                &write.fields,
                &json!({}),
                revision.as_deref(),
                operation,
            )
            .await?,
        );
    }
    if let Some(mark) = &write.copy_blob {
        let inline = match &write.copy_inline {
            Some(inline) => Some(fetched(source, &write.key, inline, "inline").await?),
            None => None,
        };
        let blob = fetched(source, &write.key, mark, "blob").await?;
        revision = Some(
            put_files(
                destination,
                write,
                revision.as_deref(),
                &blob,
                inline.as_deref(),
            )
            .await?,
        );
    } else if let Some(mark) = &write.copy_inline {
        let bytes = fetched(source, &write.key, mark, "inline").await?;
        revision = Some(put_inline(destination, write, revision.as_deref(), mark, &bytes).await?);
    }
    if write.clear_blob {
        revision = Some(clear(destination, write, revision.as_deref(), "blob").await?);
    }
    if write.clear_inline {
        revision = Some(clear(destination, write, revision.as_deref(), "inline").await?);
    }
    let _latest = revision;
    Ok(())
}

async fn apply_remove(destination: &TargetStore<'_>, remove: &Remove) -> Result<(), String> {
    let mut revision = remove.revision.clone();
    if remove.clear_blob {
        revision = clear_row(destination, remove, &revision, "blob").await?;
    }
    if remove.clear_inline {
        revision = clear_row(destination, remove, &revision, "inline").await?;
    }
    mutate(
        destination,
        &remove.key,
        &BTreeMap::new(),
        &remove.alternates,
        Some(&revision),
        "delete",
    )
    .await?;
    Ok(())
}

async fn fetched(
    source: &TargetStore<'_>,
    key: &RecordKey,
    mark: &FileMark,
    placement: &str,
) -> Result<Vec<u8>, String> {
    let schema = key.qualified_schema();
    let (sha, bytes) = source
        .fetch_file(&schema, &key.hash, key.range.as_deref(), placement)
        .await?;
    if sha != mark.sha256 {
        return Err("file bytes do not match the record".into());
    }
    Ok(bytes)
}

async fn put_files(
    destination: &TargetStore<'_>,
    write: &Write,
    revision: Option<&str>,
    blob: &[u8],
    inline: Option<&[u8]>,
) -> Result<String, String> {
    let mark = write.copy_blob.as_ref().ok_or("file bytes are missing")?;
    let operation = if write.creating { "create" } else { "replace" };
    let mut body = json!({
        "mutation": mutation_body(write, revision, operation)?,
        "bytes_b64": base64::engine::general_purpose::STANDARD.encode(blob),
        "name": mark.name,
        "media_type": mark.media_type,
        "placement": "blob",
    });
    if let (Some(bytes), Some(inline_mark)) = (inline, write.copy_inline.as_ref()) {
        body["thumbnail"] = json!({
            "bytes_b64": base64::engine::general_purpose::STANDARD.encode(bytes),
            "name": inline_mark.name,
            "media_type": inline_mark.media_type,
        });
    }
    record_revision(&destination.put_file(&body).await?)
}

async fn put_inline(
    destination: &TargetStore<'_>,
    write: &Write,
    revision: Option<&str>,
    mark: &FileMark,
    bytes: &[u8],
) -> Result<String, String> {
    let operation = if write.creating { "create" } else { "replace" };
    let body = json!({
        "mutation": mutation_body(write, revision, operation)?,
        "bytes_b64": base64::engine::general_purpose::STANDARD.encode(bytes),
        "name": mark.name,
        "media_type": mark.media_type,
        "placement": "inline",
    });
    record_revision(&destination.put_file(&body).await?)
}

async fn clear(
    destination: &TargetStore<'_>,
    write: &Write,
    revision: Option<&str>,
    placement: &str,
) -> Result<String, String> {
    let body = json!({
        "mutation": row_mutation(
            &write.key,
            &write.fields,
            &json!({}),
            false,
            revision,
            "replace",
        )?,
        "placement": placement,
    });
    record_revision(&destination.delete_file(&body).await?)
}

async fn clear_row(
    destination: &TargetStore<'_>,
    remove: &Remove,
    revision: &str,
    placement: &str,
) -> Result<String, String> {
    let body = json!({
        "mutation": row_mutation(&remove.key, &remove.fields, &remove.alternates, false, Some(revision), "replace")?,
        "placement": placement,
    });
    record_revision(&destination.delete_file(&body).await?)
}

async fn mutate(
    destination: &TargetStore<'_>,
    key: &RecordKey,
    fields: &BTreeMap<String, Value>,
    alternates: &Value,
    revision: Option<&str>,
    operation: &str,
) -> Result<String, String> {
    let body = row_mutation(
        key,
        fields,
        alternates,
        operation == "create",
        revision,
        operation,
    )?;
    record_revision(&destination.mutate(&body).await?)
}

fn mutation_body(write: &Write, revision: Option<&str>, operation: &str) -> Result<Value, String> {
    row_mutation(
        &write.key,
        &write.fields,
        &json!({}),
        write.creating,
        revision,
        operation,
    )
}

fn row_mutation(
    key: &RecordKey,
    fields: &BTreeMap<String, Value>,
    alternates: &Value,
    creating: bool,
    revision: Option<&str>,
    operation: &str,
) -> Result<Value, String> {
    let expected = if creating || operation == "create" {
        json!({"type": "absent"})
    } else {
        json!({"type": "revision", "revision": revision.ok_or("saved query replace has no revision")?})
    };
    let mut key_value = Map::new();
    key_value.insert("hash".to_owned(), json!(key.hash));
    if let Some(range) = &key.range {
        key_value.insert("range".to_owned(), json!(range));
    }
    Ok(json!({
        "type": "mutation",
        "schema": key.qualified_schema(),
        "key_value": key_value,
        "record": fields,
        "alternates": alternates,
        "mutation_type": operation,
        "expected": expected,
        "durability": "durable",
        "convergence": "sync",
    }))
}

fn record_revision(response: &Value) -> Result<String, String> {
    response
        .get("record_revision")
        .and_then(Value::as_str)
        .filter(|revision| !revision.is_empty())
        .map(str::to_owned)
        .ok_or("mutation returned no record revision".to_owned())
}
