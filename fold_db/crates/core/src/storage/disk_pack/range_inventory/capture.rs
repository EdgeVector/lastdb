use super::source::Source;
use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use std::collections::BTreeSet;

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
    let main = &source.main;
    for collection in collections(main, limits)? {
        super::run_pages::visit(main, &collection, b"", &[255], limits, |key, value| {
            push(&mut records, &mut bytes, limits, &collection, &key, value)
        })?;
    }
    records.sort_unstable_by(|a, b| (&a.collection, &a.key).cmp(&(&b.collection, &b.key)));
    Ok(records)
}

fn collections(
    main: &laststore::disk_run::home::RunRead,
    limits: InventoryLimits,
) -> StorageResult<BTreeSet<String>> {
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
    let mut registry_entries = 0;
    super::run_pages::visit(
        main,
        "__native_home",
        b"namespaces\0",
        b"namespaces\x01",
        limits,
        |key, value| {
            registry_entries += 1;
            if value != [1] || registry_entries > 10000 {
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
            Ok(())
        },
    )?;
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
    if value.len() > body_limit(records, *bytes, limits, collection, key)? {
        return Err(invalid(
            "source metadata inventory exceeds its count or byte limit",
        ));
    }
    *bytes += collection.len() + key.len() + value.len();
    records.push(MetadataRecord {
        collection: collection.into(),
        key: key.into(),
        value,
    });
    Ok(())
}

fn body_limit(
    records: &[MetadataRecord],
    bytes: usize,
    limits: InventoryLimits,
    collection: &str,
    key: &str,
) -> StorageResult<usize> {
    if records.len() >= limits.max_records || key.len() > limits.max_key_bytes {
        return Err(invalid(
            "source metadata inventory exceeds its count or key limit",
        ));
    }
    limits
        .max_metadata_bytes
        .checked_sub(bytes)
        .and_then(|available| available.checked_sub(collection.len()))
        .and_then(|available| available.checked_sub(key.len()))
        .ok_or_else(|| invalid("source metadata inventory exceeds its byte limit"))
}
