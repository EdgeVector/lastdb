//! Raw exact-key reads and finite read-budget accounting.

use super::physical::PhysicalRead;
use super::read::ReadContext;
use super::*;

impl ReadContext {
    pub(in crate::storage::disk_pack) fn record_stage(&self, stage: &'static str, items: usize) {
        if !tracing::enabled!(target: "lastdb::native_read_budget", tracing::Level::DEBUG) {
            return;
        }
        let call = self
            .call
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let costs = call.costs();
        let limits = call.budget();
        tracing::debug!(
            target: "lastdb::native_read_budget",
            stage,
            items,
            examined_keys = costs.examined_keys,
            key_limit = limits.keys,
            charged_bytes = costs.budget_bytes,
            "native preparation cumulative read costs"
        );
    }

    /// Charge required memory results before their body is copied.
    pub(in crate::storage::disk_pack) fn account_memory_record(
        &self,
        call: &mut PhysicalRead,
        key_bytes: usize,
        body_bytes: usize,
    ) -> StorageResult<()> {
        let result = key_bytes
            .checked_add(body_bytes)
            .ok_or_else(|| StorageError::ReadResourceLimit {
                detail: "native memory record length overflow".into(),
            })
            .and_then(|bytes| {
                self.require_room(call, 1, bytes as u64)?;
                call.account_memory_record(key_bytes, body_bytes)
                    .map_err(|error| call_error(error, call))
            });
        self.remembered(result)
    }

    fn require_room(&self, call: &PhysicalRead, keys: u64, bytes: u64) -> StorageResult<()> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        let costs = call.costs();
        let limits = call.budget();
        if costs
            .examined_keys
            .checked_add(keys)
            .is_none_or(|n| n > limits.keys)
            || costs
                .budget_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > limits.bytes)
        {
            return Err(StorageError::ReadResourceLimit {
                detail: format!(
                    "native required work: examined_keys={}+{keys}/{}, charged_bytes={}+{bytes}/{}",
                    costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
                ),
            });
        }
        Ok(())
    }
}

fn call_error(error: laststore::Error, call: &PhysicalRead) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let costs = call.costs();
    let limits = call.budget();
    StorageError::ReadResourceLimit {
        detail: format!(
            "native call: examined_keys={}/{}, charged_bytes={}/{}",
            costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
        ),
    }
}

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
        let result = self.raw_many_inner(context, coordinates, pending);
        context.remembered(result)
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
        let mut call = context
            .call
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            context.require_room(&call, coordinates.len() as u64, 0)?;
            let mut out = Vec::with_capacity(coordinates.len());
            for chunk in coordinates.chunks(10_000) {
                out.extend(self.read_chunk(context, &mut call, chunk, pending)?);
            }
            Ok(out)
        })();
        context.remembered(result)
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

fn trace_families(coordinates: &[(String, String)]) {
    if !tracing::enabled!(target: "lastdb::native_read_family", tracing::Level::DEBUG) {
        return;
    }
    let mut counts = std::collections::BTreeMap::new();
    for (collection, key) in coordinates {
        let family = match collection.as_str() {
            crate::storage::format::SCHEMAS_NAMESPACE => "schema",
            crate::storage::format::SCHEMA_STATES_NAMESPACE => "schema_state",
            _ => record_family(key),
        };
        *counts.entry(family).or_insert(0usize) += 1;
    }
    for (family, keys) in counts {
        tracing::debug!(target: "lastdb::native_read_family", family, keys,
            "native exact read families");
    }
}

fn record_family(key: &str) -> &'static str {
    let raw = crate::storage::disk_pack::native_routing::decode(key).unwrap_or_default();
    let family = std::str::from_utf8(&raw)
        .ok()
        .and_then(|key| key.split([':', '\0']).next());
    match family {
        Some("rk") => "record",
        Some("ak") => "alternate_key",
        _ => "other",
    }
}
