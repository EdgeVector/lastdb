use super::source::{budget, Source};
use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::storage::disk_pack::map;
use laststore::disk_pack::maintenance::visit_generation_streaming;
use std::collections::BTreeSet;
use std::io::Read;

const PAGE: usize = 128;
const MAX_RECORD: usize = 4 * 1024 * 1024;

fn selected(collection: &str) -> bool {
    // Atom bodies/counters and CAS payloads cannot define molecule coordinates.
    // Preserve them by reference in the next conversion stage; do not load them.
    !matches!(collection, "atoms" | "cas_blobs" | "__native_home")
}

pub(super) fn collect(
    source: &mut Source,
    limits: InventoryLimits,
) -> StorageResult<Vec<MetadataRecord>> {
    let mut records = Vec::new();
    let mut bytes = 0_usize;
    match source {
        Source::Packed { home, reader } => {
            visit_generation_streaming(home.path(), reader.current(), |record| {
                let Some(body) = record.body else {
                    return Ok(());
                };
                if !selected(record.collection) {
                    std::io::copy(body, &mut std::io::sink())?;
                    return Ok(());
                }
                if record.body_bytes > MAX_RECORD as u64 {
                    return Err(laststore::Error::Config(
                        "range inventory metadata record exceeds its bound".into(),
                    ));
                }
                let mut value = Vec::new();
                body.take(MAX_RECORD as u64 + 1).read_to_end(&mut value)?;
                if value.len() as u64 != record.body_bytes {
                    return Err(laststore::Error::Config(
                        "range inventory metadata record is incomplete".into(),
                    ));
                }
                push(
                    &mut records,
                    &mut bytes,
                    limits,
                    record.collection,
                    record.key,
                    value,
                )
                .map_err(|error| laststore::Error::Config(error.to_string()))
            })
            .map_err(map)?;
        }
        Source::Runs { main, .. } => {
            for collection in collections(main, limits)? {
                let mut read = main.fork(budget(
                    limits.max_metadata_bytes.saturating_mul(2),
                    limits.max_records.saturating_mul(8),
                ));
                let mut cursor = read
                    .administrative_range(&collection, b"", &[255])
                    .map_err(map)?;
                loop {
                    let page = cursor.next_page(PAGE).map_err(map)?;
                    for (key, value) in page.records {
                        push(&mut records, &mut bytes, limits, &collection, &key, value)?;
                    }
                    if page.exhausted {
                        break;
                    }
                }
            }
        }
    }
    records.sort_unstable_by(|a, b| (&a.collection, &a.key).cmp(&(&b.collection, &b.key)));
    Ok(records)
}

fn collections(
    main: &laststore::disk_run::home::RunRead,
    limits: InventoryLimits,
) -> StorageResult<BTreeSet<String>> {
    let mut registry = main.fork(budget(
        limits.max_metadata_bytes,
        limits.max_records.saturating_mul(8),
    ));
    let mut cursor = registry
        .administrative_range("__native_home", b"namespaces\0", b"namespaces\x01")
        .map_err(map)?;
    let mut names: BTreeSet<String> = [
        "tips",
        "indexes",
        "proteins",
        "molecule_ref_edges",
        "metadata",
        "schemas",
        "molecule_keys",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    loop {
        let page = cursor.next_page(PAGE).map_err(map)?;
        for (key, value) in page.records {
            if value != [1] || names.len() >= 10000 {
                return Err(invalid(
                    "source namespace registry is invalid or exceeds its bound",
                ));
            }
            let name = key
                .strip_prefix("namespaces\0")
                .ok_or_else(|| invalid("source namespace key is invalid"))?;
            if selected(name) && name != "main" {
                names.insert(name.to_owned());
            }
        }
        if page.exhausted {
            break;
        }
    }
    Ok(names)
}

fn push(
    records: &mut Vec<MetadataRecord>,
    bytes: &mut usize,
    limits: InventoryLimits,
    collection: &str,
    key: &str,
    value: Vec<u8>,
) -> StorageResult<()> {
    if matches!(collection, "field_tips" | "blob_ref_edges") {
        return Err(invalid("source contains a retired physical metadata alias"));
    }
    let next = bytes
        .checked_add(collection.len())
        .and_then(|n| n.checked_add(key.len()))
        .and_then(|n| n.checked_add(value.len()));
    if records.len() >= limits.max_records
        || key.len() > limits.max_key_bytes
        || value.len() > MAX_RECORD
        || next.is_none_or(|n| n > limits.max_metadata_bytes)
    {
        return Err(invalid(
            "source metadata inventory exceeds its count or byte limit",
        ));
    }
    *bytes = next.expect("checked above");
    records.push(MetadataRecord {
        collection: collection.into(),
        key: key.into(),
        value,
    });
    Ok(())
}
