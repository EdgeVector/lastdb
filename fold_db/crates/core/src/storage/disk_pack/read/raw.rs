//! Physical dependency capture accompanies every bounded native read.

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
        laststore::record_requested_keys(coordinates.len() as u64);
        let result = self.raw_many_inner(context, coordinates, pending);
        context.remember(&result);
        if let Ok(values) = &result {
            context.note_points(coordinates, values);
        }
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
        call: &mut HomeRead,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let mut out = vec![None; coordinates.len()];
        let mut misses = Vec::new();
        for (slot, (collection, key)) in coordinates.iter().enumerate() {
            if let Some(record) = context.overlay_record(pending, collection, key) {
                context.account_memory_record(
                    call,
                    key.len(),
                    record.body.as_ref().map_or(0, Vec::len),
                )?;
                out[slot] = record.body.clone();
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
            out[slot] = result.map_err(|error| call_error(error, call))?;
        }
        Ok(out)
    }
}
