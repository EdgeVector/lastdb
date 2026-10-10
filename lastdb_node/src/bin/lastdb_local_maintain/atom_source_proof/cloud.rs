//! Strict normal snapshot frontier and all durable org/unconfirmed replay roots.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::storage::traits::KvStore;
use fold_db::sync::engine::{
    decode_offline_pin_log_row, offline_pin_log_restore_frontier_key, OfflinePinLogRow,
    PIN_LOG_NAMESPACE,
};
use fold_db::sync::log::{LogOp, MutationEnvelope};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    version: u32,
    #[serde(deserialize_with = "unique_writers")]
    by_writer: BTreeMap<String, u64>,
    #[serde(default)]
    mode: Option<fold_db::sync::engine::BackupRestoreMode>,
}

pub(super) async fn snapshot(
    opened: &HomeStore,
    published: &BTreeMap<String, u64>,
) -> Result<BTreeMap<String, u64>, String> {
    let raw = opened
        .base
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let key = offline_pin_log_restore_frontier_key();
    let bytes = raw
        .get(key)
        .await
        .map_err(err)?
        .ok_or("target atom proof requires a prior authoritative normal snapshot frontier")?;
    if !matches!(
        decode_offline_pin_log_row(key, &bytes)?,
        OfflinePinLogRow::RestoreFrontier
    ) {
        return Err("snapshot frontier has the wrong key kind".into());
    }
    let marker: Marker = serde_json::from_slice(&bytes).map_err(err)?;
    if marker.version != 1
        || marker.mode.is_some()
        || marker.by_writer.is_empty()
        || &marker.by_writer != published
    {
        return Err("normal snapshot map differs from the stopped personal published map".into());
    }
    Ok(marker.by_writer)
}

fn unique_writers<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<BTreeMap<String, u64>, D::Error> {
    struct Unique;
    impl<'de> serde::de::Visitor<'de> for Unique {
        type Value = BTreeMap<String, u64>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("unique nonempty snapshot writers")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut writers = BTreeMap::new();
            while let Some((writer, frontier)) = map.next_entry::<String, u64>()? {
                if writer.trim().is_empty() || writers.insert(writer, frontier).is_some() {
                    return Err(serde::de::Error::custom(
                        "invalid or duplicate snapshot writer",
                    ));
                }
            }
            Ok(writers)
        }
    }
    decoder.deserialize_map(Unique)
}

pub(super) async fn roots(
    opened: &HomeStore,
    targets: &BTreeSet<String>,
    links: &mut sources::Links,
    facts: &mut Facts,
) -> Result<(), String> {
    let raw = opened
        .base
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let seam = opened
        .store
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let mut walker = crate::reap::walk::Walker::new(Arc::clone(&raw));
    while let Some(page) = walker.next_page().await.map_err(err)? {
        let keys: Vec<_> = page.rows.iter().map(|row| row.0.clone()).collect();
        let values = read_page(&*raw, &*seam, &page.rows, &keys).await?;
        for ((key, _), plain) in page.rows.into_iter().zip(values) {
            let records = match decode_offline_pin_log_row(&key, &plain)? {
                OfflinePinLogRow::Entry(record) => vec![record],
                OfflinePinLogRow::CaptureReceipt(records) => records,
                _ => Vec::new(),
            };
            for record in records {
                if matches!(record.entry.op, LogOp::Unknown { .. }) {
                    return Err("unknown durable cloud operation prevents atom proof".into());
                }
                if record.target_id == "personal"
                    && facts
                        .snapshot_writer_map
                        .get(&record.writer_id)
                        .is_some_and(|frontier| *frontier >= record.frontier_after)
                {
                    facts.count("confirmed_personal_cloud_roots_omitted");
                    continue;
                }
                facts.count("org_or_unconfirmed_cloud_records");
                operation(&record.entry.op, &key, targets, links, facts)?;
            }
        }
    }
    Ok(())
}

async fn read_page(
    raw: &dyn KvStore,
    seam: &dyn KvStore,
    rows: &[(Vec<u8>, Vec<u8>)],
    keys: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, String> {
    let decrypt: Vec<_> = keys
        .iter()
        .filter(|key| key.as_slice() != offline_pin_log_restore_frontier_key())
        .cloned()
        .collect();
    let (stored, values) =
        tokio::join!(raw.get_many(keys.to_vec()), seam.get_many(decrypt.clone()));
    let stored = stored.map_err(err)?;
    let values = values.map_err(err)?;
    if stored.len() != keys.len() || values.len() != decrypt.len() {
        return Err("cloud source batch count differs".into());
    }
    let mut values = values.into_iter();
    let mut plain = Vec::with_capacity(keys.len());
    for (((key, physical), stored), expected) in rows.iter().zip(stored).zip(keys) {
        if key != expected || stored.as_ref() != Some(physical) {
            return Err("physical cloud source differs from its keyed bytes".into());
        }
        let value = if key.as_slice() == offline_pin_log_restore_frontier_key() {
            physical.clone()
        } else {
            values
                .next()
                .flatten()
                .ok_or("physical cloud source is hidden from the reader")?
        };
        plain.push(value);
    }
    Ok(plain)
}

fn operation(
    op: &LogOp,
    journal_key: &[u8],
    targets: &BTreeSet<String>,
    links: &mut sources::Links,
    facts: &mut Facts,
) -> Result<(), String> {
    match op {
        LogOp::MutationIntent { mutations } => {
            for mutation in mutations {
                intent(mutation, journal_key, targets, facts)?;
            }
        }
        LogOp::Put {
            namespace,
            key,
            value,
        } => put(namespace, key, value, journal_key, targets, links, facts)?,
        LogOp::BatchPut { namespace, items } => {
            for (key, value) in items {
                put(namespace, key, value, journal_key, targets, links, facts)?;
            }
        }
        LogOp::LogicalCommit { changes } => {
            for change in changes {
                if let Some(value) = &change.value {
                    put(
                        &change.namespace,
                        &change.key,
                        value,
                        journal_key,
                        targets,
                        links,
                        facts,
                    )?;
                } else {
                    STANDARD.decode(&change.key).map_err(err)?;
                }
            }
        }
        LogOp::PhysicalDigest { items, .. } => {
            for (key, hash) in items {
                STANDARD.decode(key).map_err(err)?;
                if STANDARD.decode(hash).map_err(err)?.len() != 32 {
                    return Err("invalid cloud physical digest".into());
                }
            }
        }
        LogOp::Delete { key, .. } => {
            STANDARD.decode(key).map_err(err)?;
        }
        LogOp::BatchDelete { keys, .. } => {
            for key in keys {
                STANDARD.decode(key).map_err(err)?;
            }
        }
        LogOp::Unknown { .. } => {
            return Err("unknown cloud operation prevents target atom proof".into())
        }
    }
    Ok(())
}

fn intent(
    mutation: &MutationEnvelope,
    key: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    if !matches!(
        mutation.mutation_type.as_str(),
        "create" | "update" | "delete" | "purge"
    ) {
        return Err("unknown cloud mutation type".into());
    }
    let scope = mutation.storage_prefix.as_deref().unwrap_or_default();
    if matches!(mutation.mutation_type.as_str(), "create" | "update")
        && mutation.fields_and_values.is_empty()
        && mutation.field_atom_uuids.is_empty()
    {
        return Err("cloud mutation has no complete field sources".into());
    }
    if matches!(mutation.mutation_type.as_str(), "create" | "update")
        && mutation.fields_and_values.iter().any(|(field, value)| {
            let expected = fold_db::atom::Atom::new(mutation.schema_name.clone(), value.clone());
            mutation.field_atom_uuids.get(field).map(String::as_str) != Some(expected.uuid())
        })
    {
        // Production replay consumes inline bodies even when the capture writer
        // cleared their UUID map. Without a complete matching field map, no target UUID
        // absence can be inferred from this replayable envelope.
        facts.count("unresolved_cloud_intent_maps");
        facts.hold_all(
            targets,
            model::hold(
                PIN_LOG_NAMESPACE,
                key,
                scope,
                "unresolved_inline_cloud_intent",
                None,
            ),
        );
    }
    for uuid in mutation.field_atom_uuids.values() {
        if uuid.is_empty() {
            return Err("cloud mutation names an empty atom".into());
        }
        facts.hold(
            targets,
            uuid,
            model::hold(
                PIN_LOG_NAMESPACE,
                key,
                scope,
                "org_or_unconfirmed_cloud_intent",
                None,
            ),
        );
    }
    Ok(())
}

fn put(
    namespace: &str,
    key: &str,
    value: &str,
    journal_key: &[u8],
    targets: &BTreeSet<String>,
    links: &mut sources::Links,
    facts: &mut Facts,
) -> Result<(), String> {
    if namespace.is_empty() {
        return Err("cloud put has an empty namespace".into());
    }
    let key = STANDARD.decode(key).map_err(err)?;
    let value = STANDARD.decode(value).map_err(err)?;
    let identity = if namespace == "lineage_reverse" {
        None
    } else {
        reader::atom_identity(&key)?
    };
    if let Some((scope, uuid)) = identity {
        // An explicit replayable body key is sufficient to retain its UUID;
        // the production header must decode and agree, even for inline bodies.
        let body = fold_db::atom::atom_row_header(&value)
            .map_err(|_| "unsupported replayable atom body")?;
        if body.get("uuid").and_then(serde_json::Value::as_str) != Some(uuid.as_str()) {
            return Err("cloud atom body identity differs from its key".into());
        }
        facts.hold(
            targets,
            &uuid,
            model::hold(
                PIN_LOG_NAMESPACE,
                journal_key,
                &scope,
                "org_or_unconfirmed_cloud_body",
                None,
            ),
        );
    } else if namespace == "atoms" {
        return Err("cloud atoms operation has an unsupported key".into());
    } else if namespace == "cas_blobs" { /* A blob body cannot retain an atom. */
    } else {
        sources::observe(namespace, &key, &value, targets, links, facts)?;
    }
    Ok(())
}
