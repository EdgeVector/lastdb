//! Export a bounded signed page from one held native read view.

use super::*;

impl PackRuntime {
    pub async fn logical_capture_export(
        self: &Arc<Self>,
        after: u64,
        limit: usize,
    ) -> StorageResult<CaptureExportPage> {
        if limit == 0 || limit > PAGE_EVENTS {
            return Err(StorageError::MutationLimit {
                detail: "logical capture export limit must be 1..=4".into(),
            });
        }
        let context = read::ReadContext::current(self).await?;
        let runtime = Arc::clone(self);
        run_blocking(move || runtime.capture_export_page(&context, after, limit)).await
    }

    fn capture_export_page(
        &self,
        context: &Arc<read::ReadContext>,
        after: u64,
        limit: usize,
    ) -> StorageResult<CaptureExportPage> {
        let capture = self
            .logical_capture
            .as_ref()
            .ok_or_else(|| invalid("logical capture is disabled"))?;
        let head = self.capture_head(context)?;
        if after > head.position.sequence {
            return Err(invalid(
                "logical capture export position is beyond its head",
            ));
        }
        let anchor = Position {
            origin: capture.identity.origin(),
            sequence: 0,
            sha256: capture.identity.anchor()?,
        };
        let mut page = CaptureExportPage {
            anchor,
            position: head.position,
            after,
            events: Vec::new(),
        };
        if after == page.position.sequence {
            return Ok(page);
        }
        let end = after
            .saturating_add(limit as u64)
            .min(page.position.sequence);
        let metadata = export_metadata(self, context, after, end, &page.position.origin)?;
        let mut previous = if after == 0 {
            page.anchor.sha256.clone()
        } else {
            metadata[0].sha256.clone()
        };
        let candidates = &metadata[usize::from(after > 0)..];
        let selected = fit_metadata(candidates)?;
        let coordinates = candidates[..selected]
            .iter()
            .map(|entry| (COLLECTION.into(), key("e", entry.sequence)))
            .collect::<Vec<_>>();
        let bodies = self.raw_many(context, &coordinates, None)?;
        for (entry, bytes) in candidates.iter().zip(bodies) {
            let bytes = bytes.ok_or_else(|| invalid("logical capture export event is absent"))?;
            let event: CaptureEvent = decode(&bytes)?;
            event.verify()?;
            let size = bounded::serialized_size(&event)?;
            if size != bytes.len()
                || event.metadata(size) != *entry
                || event.previous_sha256 != previous
            {
                return Err(invalid("logical capture export event and metadata differ"));
            }
            previous = event.sha256.clone();
            page.events.push(event);
        }
        if page
            .events
            .last()
            .is_some_and(|event| event.sequence == page.position.sequence)
            && previous != page.position.sha256
        {
            return Err(invalid(
                "logical capture export does not reach its durable head",
            ));
        }
        bounded::serialized_size_with_limit(&page.events, MAX_EVENT_BYTES)?;
        bounded::serialized_size_with_limit(&page, MAX_EVENT_BYTES + PAGE_ENVELOPE_BYTES)?;
        Ok(page)
    }
}

fn export_metadata(
    runtime: &PackRuntime,
    context: &Arc<read::ReadContext>,
    after: u64,
    end: u64,
    origin: &str,
) -> StorageResult<Vec<EventMetadata>> {
    let first = after.max(1);
    let coordinates = (first..=end)
        .map(|sequence| (COLLECTION.into(), key("m", sequence)))
        .collect::<Vec<_>>();
    let values = runtime.raw_many(context, &coordinates, None)?;
    values
        .into_iter()
        .enumerate()
        .map(|(offset, bytes)| {
            let entry: EventMetadata = decode(
                &bytes.ok_or_else(|| invalid("logical capture export metadata is absent"))?,
            )?;
            if entry.origin != origin || entry.sequence != first + offset as u64 {
                return Err(invalid("logical capture export metadata identity differs"));
            }
            unhex::<32>(&entry.sha256)?;
            unhex::<32>(&entry.previous_sha256)?;
            verify::mutation_ids(&entry.mutation_ids)?;
            if entry.outcome_count > MAX_OUTCOMES {
                return Err(invalid(
                    "logical capture export metadata outcome cap differs",
                ));
            }
            Ok(entry)
        })
        .collect()
}

fn fit_metadata(entries: &[EventMetadata]) -> StorageResult<usize> {
    let mut bytes = 2usize;
    let mut count = 0;
    for entry in entries {
        let size = usize::try_from(entry.encoded_event_bytes)
            .map_err(|_| invalid("logical capture export event size differs"))?;
        if size == 0 || size > MAX_EVENT_BYTES - 2 {
            return Err(invalid("logical capture export event exceeds its byte cap"));
        }
        let next = bytes + size + usize::from(count > 0);
        if next > MAX_EVENT_BYTES {
            break;
        }
        bytes = next;
        count += 1;
    }
    Ok(count)
}
