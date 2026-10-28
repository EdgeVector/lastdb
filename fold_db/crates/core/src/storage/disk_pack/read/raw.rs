//! Ordinary raw reads use disk; private preparation can read its own flush log.

use super::*;

impl PackRuntime {
    pub(crate) fn record_read_stage(&self, stage: &'static str, items: usize) {
        if let Some(context) = ReadContext::captured(self) {
            context.record_stage(stage, items);
        }
    }

    pub(in crate::storage::disk_pack) fn raw_get(
        &self,
        context: &Arc<ReadContext>,
        collection: &str,
        key: &str,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .raw_many(
                context,
                &[(collection.to_string(), key.to_string())],
                pending,
            )?
            .pop()
            .expect("one native coordinate"))
    }

    pub(in crate::storage::disk_pack) fn raw_many(
        &self,
        context: &Arc<ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        trace_families(coordinates);
        laststore::record_requested_keys(coordinates.len() as u64);
        let result = self.raw_many_inner(context, coordinates, pending);
        context.remember(&result);
        result
    }

    fn raw_many_inner(
        &self,
        context: &Arc<ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        context.validate_owner(pending)?;
        let maximum = if context.owns_write(pending) {
            40_000
        } else {
            10_000
        };
        if coordinates.len() > maximum {
            return Err(StorageError::ReadResourceLimit {
                detail: format!("native batch exceeds {maximum} keys"),
            });
        }
        let mut call = crate::read_receipts::lock(&context.call);
        let result = (|| {
            context.require_room(&call, coordinates.len() as u64, 0)?;
            let mut out = Vec::with_capacity(coordinates.len());
            // One immutable call and PendingBatch cover every ordered chunk.
            // Both engine batch readers retain their 10,000-slot input cap.
            for chunk in coordinates.chunks(10_000) {
                out.extend(self.read_chunk(context, &mut call, chunk, pending)?);
            }
            Ok(out)
        })();
        context.remember(&result);
        result
    }

    fn read_chunk(
        &self,
        context: &ReadContext,
        call: &mut PhysicalRead,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let mut out = vec![None; coordinates.len()];
        let mut misses = Vec::new();
        for (slot, coordinate) in coordinates.iter().enumerate() {
            let (collection, key) = coordinate;
            if let Some(record) = context.overlay_record(pending, collection, key) {
                context.account_memory_record(
                    call,
                    key.len(),
                    record.body.as_ref().map_or(0, Vec::len),
                )?;
                out[slot] = record.body.clone();
            } else if let Some(value) = context.reuse_disk_value(call, coordinate)? {
                out[slot] = value;
            } else {
                misses.push(slot);
            }
        }
        let addresses: Vec<_> = misses
            .iter()
            .map(|slot| coordinates[*slot].clone())
            .collect();
        let results = call
            .get_many(&addresses)
            .map_err(|error| call_error(error, call))?;
        for (slot, result) in misses.into_iter().zip(results) {
            let value = result.map_err(|error| call_error(error, call))?;
            context.remember_disk_value(&coordinates[slot], value.as_deref())?;
            out[slot] = value;
        }
        Ok(out)
    }
}

/// Opt-in diagnostics name only fixed record families, never user keys or data.
fn trace_families(coordinates: &[(String, String)]) {
    if !tracing::enabled!(target: "lastdb::native_read_family", tracing::Level::DEBUG) {
        return;
    }
    let mut counts = std::collections::BTreeMap::new();
    for (collection, key) in coordinates {
        let family = match collection.as_str() {
            "schemas" => "schema",
            "schema_states" => "schema_state",
            "schema_superseded_by" => "schema_successor",
            "molecule_keys" => "molecule_encryption_keys",
            "share_subscriptions" => "receiver_registry",
            "db_catalog" => "database_membership",
            _ => {
                let raw =
                    crate::storage::disk_pack::native_routing::decode(key).unwrap_or_default();
                [
                    b"hcu".as_slice(),
                    b"mgp",
                    b"mgr",
                    b"mgd",
                    b"mh",
                    b"rdel",
                    b"mk",
                    b"atom",
                ]
                .into_iter()
                .zip([
                    "conflict_control",
                    "generation_pointer",
                    "generation_record",
                    "generation_delete",
                    "molecule_header",
                    "record_delete",
                    "tip",
                    "atom",
                ])
                .find_map(|(prefix, family)| {
                    raw.windows(prefix.len())
                        .any(|window| window == prefix)
                        .then_some(family)
                })
                .unwrap_or("other")
            }
        };
        *counts.entry(family).or_insert(0usize) += 1;
    }
    for (family, keys) in counts {
        tracing::debug!(target: "lastdb::native_read_family", family, keys,
            "native exact read families");
    }
}
