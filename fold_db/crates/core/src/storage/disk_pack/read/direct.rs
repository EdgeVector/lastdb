//! Batched typed preparation reads share the captured root and publication epoch.

use super::*;
use serde::de::DeserializeOwned;
use std::any::Any;
use write_view::{Coordinate, WriteViewCounts};

impl PackRuntime {
    /// Native preparation consumes typed hits directly. Other callers retain
    /// their existing read route, including the CAS encryption decorator.
    pub(crate) async fn direct_write_items<T: DeserializeOwned + Send + Sync + 'static>(
        self: &Arc<Self>,
        namespace: &str,
        keys: &[String],
        decode: fn(&[u8]) -> StorageResult<T>,
    ) -> StorageResult<Option<Vec<Option<T>>>> {
        let Some(pending) = transaction::PendingBatch::current(self) else {
            return Ok(None);
        };
        if namespace == super::physical::CAS_COLLECTION {
            return Ok(None);
        }
        if keys.len() > 10_000 {
            return Err(StorageError::ReadResourceLimit {
                detail: "native typed request exceeds 10000 keys".into(),
            });
        }
        for key in keys {
            crate::storage::current_keys::validate(namespace, key.as_bytes())?;
        }
        let context = ReadContext::current(self).await?;
        context.validate_owner(Some(&pending))?;
        let coordinates: Vec<_> = keys
            .iter()
            .map(|key| super::kv::coordinate(namespace, key.as_bytes()))
            .collect();
        let request = DirectRequest {
            runtime: Arc::clone(self),
            context: Arc::clone(&context),
            pending,
            coordinates: coordinates.clone(),
        };
        let result = super::run_blocking(move || request.read(decode)).await;
        context.remember(&result);
        let values = result?;
        for ((requested, coordinate), value) in keys.iter().zip(coordinates).zip(&values) {
            super::logical_paths::source::note_read_source(
                Arc::clone(self),
                requested.as_bytes(),
                value
                    .as_ref()
                    .map(|_| (coordinate.0.as_str(), coordinate.1.as_str())),
            )?;
        }
        Ok(Some(values))
    }
}

struct DirectRequest {
    runtime: Arc<PackRuntime>,
    context: Arc<ReadContext>,
    pending: Arc<transaction::PendingBatch>,
    coordinates: Vec<Coordinate>,
}

impl DirectRequest {
    fn read<T: Any + Send + Sync>(
        &self,
        decode: fn(&[u8]) -> StorageResult<T>,
    ) -> StorageResult<Vec<Option<T>>> {
        let selected = self.warm()?;
        let misses: Vec<_> = selected
            .iter()
            .enumerate()
            .filter_map(|(at, value)| value.is_none().then_some(at))
            .collect();
        let coordinates: Vec<_> = misses
            .iter()
            .map(|&at| self.coordinates[at].clone())
            .collect();
        let raw = self
            .runtime
            .raw_many_disk(&self.context, &coordinates, &self.pending)?;
        if raw.len() != misses.len() {
            return Err(unavailable());
        }
        let mut values: Vec<_> = selected
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect();
        let mut lengths = Vec::with_capacity(raw.len());
        for (at, bytes) in misses.iter().copied().zip(raw) {
            lengths.push(bytes.as_ref().map_or(0, Vec::len));
            values[at] = bytes.as_deref().map(decode).transpose()?;
        }
        self.retain(&values, &misses, &lengths)?;
        Ok(values)
    }

    fn warm<T: Any + Send + Sync>(&self) -> StorageResult<Vec<Option<Option<T>>>> {
        let mut call = crate::read_receipts::lock(&self.context.call);
        self.context
            .require_room(&call, self.coordinates.len() as u64, 0)?;
        let mut selected: Vec<Option<Option<T>>> =
            (0..self.coordinates.len()).map(|_| None).collect();
        let view = self.runtime.view.read().map_err(|_| unavailable())?;
        if view.revision != self.context.revision {
            return Ok(selected);
        }
        let mut core = self.runtime.core.lock().map_err(|_| unavailable())?;
        let mut counts = WriteViewCounts::default();
        for (coordinate, selected) in self.coordinates.iter().zip(&mut selected) {
            if self
                .context
                .overlay_record(Some(&self.pending), &coordinate.0, &coordinate.1)
                .is_none()
            {
                *selected = write_view::direct::select(
                    &mut core,
                    &self.context,
                    &mut call,
                    coordinate,
                    &mut counts,
                )?;
            }
        }
        laststore::record_requested_keys(
            selected.iter().filter(|value| value.is_some()).count() as u64
        );
        self.context.note_write_view(&counts)?;
        Ok(selected)
    }

    fn retain<T: Any>(
        &self,
        values: &[Option<T>],
        misses: &[usize],
        lengths: &[usize],
    ) -> StorageResult<()> {
        let view = self.runtime.view.read().map_err(|_| unavailable())?;
        if view.revision != self.context.revision {
            return Ok(());
        }
        let mut core = self.runtime.core.lock().map_err(|_| unavailable())?;
        for (&at, &bytes) in misses.iter().zip(lengths) {
            let coordinate = &self.coordinates[at];
            // A private or accepted result is never a fact about the disk root.
            if self
                .context
                .overlay_record(Some(&self.pending), &coordinate.0, &coordinate.1)
                .is_none()
            {
                write_view::direct::retain(&mut core, coordinate, values[at].as_ref(), bytes);
            }
        }
        Ok(())
    }
}

fn unavailable() -> StorageError {
    StorageError::BackendError("native direct typed read is unavailable".into())
}
