//! Explicit stream/status tools read the log; ordinary product queries never do.

use super::model::Head;
use super::*;

impl PackRuntime {
    pub async fn logical_capture_status(self: &Arc<Self>) -> StorageResult<CaptureStatus> {
        if !self.logical_capture_enabled() {
            return Ok(CaptureStatus {
                enabled: false,
                position: None,
                encoded_backlog_bytes: 0,
                backlog_cap_bytes: BACKLOG_CAP_BYTES,
                recent: Vec::new(),
            });
        }
        let context = read::ReadContext::current(self).await?;
        let runtime = Arc::clone(self);
        run_blocking(move || {
            let head = runtime.capture_head(&context)?;
            let first = head
                .position
                .sequence
                .saturating_sub(RECENT_ENTRIES - 1)
                .max(1);
            let coordinates: Vec<_> = (first..=head.position.sequence)
                .map(|sequence| (COLLECTION.to_string(), key("m", sequence)))
                .collect();
            let values = runtime.raw_many(&context, &coordinates, None)?;
            let mut recent = Vec::with_capacity(values.len());
            for (offset, bytes) in values.into_iter().enumerate() {
                let entry: EventMetadata =
                    decode(&bytes.ok_or_else(|| invalid("logical capture metadata is absent"))?)?;
                if entry.sequence != first + offset as u64 || entry.origin != head.position.origin {
                    return Err(invalid("logical capture metadata identity differs"));
                }
                recent.push(entry);
            }
            runtime.validate_recent(&head, &recent)?;
            Ok(CaptureStatus {
                enabled: true,
                position: Some(head.position),
                encoded_backlog_bytes: head.encoded_backlog_bytes,
                backlog_cap_bytes: BACKLOG_CAP_BYTES,
                recent,
            })
        })
        .await
    }

    pub(super) fn capture_head(&self, context: &Arc<read::ReadContext>) -> StorageResult<Head> {
        let capture = self
            .logical_capture
            .as_ref()
            .ok_or_else(|| invalid("logical capture is disabled"))?;
        let bytes = self
            .raw_get(context, COLLECTION, &head_key(), None)?
            .ok_or_else(|| invalid("logical capture head is absent"))?;
        let head: Head = decode(&bytes)?;
        head.validate(&capture.identity)?;
        Ok(head)
    }

    fn validate_recent(&self, head: &Head, recent: &[EventMetadata]) -> StorageResult<()> {
        for entry in recent {
            unhex::<32>(&entry.sha256)?;
            unhex::<32>(&entry.previous_sha256)?;
        }
        for pair in recent.windows(2) {
            if pair[1].previous_sha256 != pair[0].sha256 {
                return Err(invalid("logical capture recent hash chain differs"));
            }
        }
        if let Some(last) = recent.last() {
            if last.sha256 != head.position.sha256 || last.sequence != head.position.sequence {
                return Err(invalid(
                    "logical capture metadata does not reach its durable head",
                ));
            }
        } else if head.position.sequence != 0 {
            return Err(invalid("logical capture recent metadata is absent"));
        }
        if recent.first().is_some_and(|entry| entry.sequence == 1) {
            let capture = self
                .logical_capture
                .as_ref()
                .ok_or_else(|| invalid("logical capture is disabled"))?;
            if recent[0].previous_sha256 != capture.identity.anchor()? {
                return Err(invalid(
                    "logical capture metadata does not start at its device anchor",
                ));
            }
        }
        Ok(())
    }
}
