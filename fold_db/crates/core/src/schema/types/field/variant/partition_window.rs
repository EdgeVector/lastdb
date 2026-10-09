//! Merge resident and durable tip windows before atom access.
use crate::db_operations::atom_store::PerKeyRecord;
use crate::db_operations::DbOperations;
use crate::schema::types::field::KeyWindow;
use crate::schema::types::{KeyValue, SchemaError};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) async fn merged_tip_window(
    db: &Arc<DbOperations>,
    molecule: &str,
    hash: &str,
    storage_prefix: Option<&str>,
    window: &KeyWindow,
    include_tombstones: bool,
) -> Result<Option<Vec<(String, PerKeyRecord)>>, SchemaError> {
    let (mut skip, limit, mut cursor) = match window {
        KeyWindow::Offset { offset, limit } => (*offset, *limit, None),
        KeyWindow::After { after, limit } if after.hash.as_deref() == Some(hash) => {
            (0, *limit, Some(after.range.clone().unwrap_or_default()))
        }
        KeyWindow::After { .. } => return Ok(None),
    };
    let codec = db.atoms().key_codec_for_molecule(molecule);
    let batch_limit = limit.clamp(128, 1024);
    let mut output = Vec::new();
    while output.len() < limit {
        let ask = skip
            .saturating_add(limit - output.len())
            .clamp(1, batch_limit);
        let page = cursor.as_ref().map_or(
            KeyWindow::Offset {
                offset: 0,
                limit: ask,
            },
            |range| KeyWindow::After {
                after: KeyValue::new(Some(hash.into()), Some(range.clone())),
                limit: ask,
            },
        );
        // Capture the overlay before I/O, as on the general read path.
        let overlay = db
            .resident()
            .partition_overlay_page(molecule, hash, cursor.as_deref(), ask);
        let Some(durable) = db
            .atoms()
            .scan_hash_tip_window(molecule, hash, storage_prefix, &page, true)
            .await?
        else {
            return Ok(None);
        };
        let durable_full = durable.len() == ask;
        let overlay_full = overlay.len() == ask;
        let mut union = BTreeMap::new();
        let mut durable_end = None;
        for (key, record) in durable {
            let (_, encoded) = crate::atom::molecule_key_codec::decode_hash_range_any(&key)
                .ok_or_else(|| SchemaError::InvalidData("invalid partition tip key".into()))?;
            let range = if codec.range_encoding().writes_ope() {
                crate::crypto::E2eKeys::ope_decode_range_plaintext(&encoded).unwrap_or(encoded)
            } else {
                encoded
            };
            durable_end = Some(range.clone());
            union.insert(range, Some((key, record)));
        }
        let overlay_end = overlay.last().map(|entry| entry.range.clone());
        for entry in overlay {
            if entry.deleted {
                union.insert(entry.range, None);
            } else if let Some(tip) = entry.tip {
                let key = codec
                    .api_hash_range_record_key(molecule, hash, &tip.range)
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
                let mut atom = crate::atom::AtomEntry::thin_with_author(
                    tip.atom_uuid,
                    tip.written_at,
                    tip.logical_counter,
                    tip.device_id,
                    tip.mutation_uuid,
                    String::new(),
                );
                atom.writer_pubkey = tip.writer_pubkey;
                union.insert(
                    entry.range,
                    Some((
                        key,
                        PerKeyRecord {
                            entry: atom,
                            meta: tip.key_metadata,
                        },
                    )),
                );
            } else {
                union.entry(entry.range).or_insert(None);
            }
        }
        let frontier = [
            durable_full.then_some(durable_end).flatten(),
            overlay_full.then_some(overlay_end).flatten(),
        ]
        .into_iter()
        .flatten()
        .min();
        let mut advanced = false;
        for (range, row) in union {
            if frontier.as_ref().is_some_and(|end| range > *end) {
                break;
            }
            cursor = Some(range);
            advanced = true;
            let Some(row) = row else {
                continue;
            };
            if !include_tombstones && row.1.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                continue;
            }
            if skip > 0 {
                skip -= 1;
            } else {
                output.push(row);
            }
            if output.len() == limit {
                break;
            }
        }
        if !advanced || frontier.is_none() {
            break;
        }
    }
    Ok(Some(output))
}
