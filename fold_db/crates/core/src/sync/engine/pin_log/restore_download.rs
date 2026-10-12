//! Bounded, ordered tail-object downloads. Requests remain owned until consumption.

use super::*;
use crate::sync::auth::S3ObjectInfo;
use crate::sync::s3::PresignedUrl;
use futures::{stream::FuturesOrdered, StreamExt};

const DOWNLOAD_CONCURRENCY: usize = 8;
const DOWNLOAD_BUFFER_BYTES: u64 = 128 * 1024 * 1024;

pub(super) struct MutationLogDownloadCandidate {
    writer_hint: String,
    object_key: String,
    pub(super) segment: MutationLogSegmentId,
    listed_size: usize,
}

pub(super) fn with_listed_sizes(
    objects: &[S3ObjectInfo],
    candidates: Vec<(String, u64, String)>,
) -> SyncResult<Vec<MutationLogDownloadCandidate>> {
    let mut sizes = HashMap::with_capacity(objects.len());
    for object in objects {
        if sizes
            .insert(object.key.as_str(), object.size)
            .is_some_and(|size| size != object.size)
        {
            return Err(SyncError::Storage(
                "mutation-log object has conflicting listed sizes".into(),
            ));
        }
    }
    candidates
        .into_iter()
        .map(|(writer_hint, through_id, object_key)| {
            let size = sizes.get(object_key.as_str()).ok_or_else(|| {
                SyncError::Storage("mutation-log candidate has no listed size".into())
            })?;
            let listed_size = usize::try_from(*size).map_err(|_| {
                SyncError::Storage("mutation-log listed size exceeds addressable memory".into())
            })?;
            let segment = mutation_log_segment_id_from_object_key(&object_key, through_id)
                .map_err(SyncError::Storage)?;
            Ok(MutationLogDownloadCandidate {
                writer_hint,
                object_key,
                segment,
                listed_size,
            })
        })
        .collect()
}

// Match the S0 scheduler: a valid oversized object gets one exclusive reservation.
// The writer's byte target is not a hard cap on single-record or historical objects.
fn download_fits(count: usize, reserved: u64, candidate: &MutationLogDownloadCandidate) -> bool {
    count == 0
        || (count < DOWNLOAD_CONCURRENCY
            && reserved <= DOWNLOAD_BUFFER_BYTES
            && candidate.listed_size as u64 <= DOWNLOAD_BUFFER_BYTES.saturating_sub(reserved))
}

struct QueueProgressGuard<'a>(Option<&'a RestoreProgress>);

impl Drop for QueueProgressGuard<'_> {
    fn drop(&mut self) {
        progress::update(self.0, |p| {
            p.queued_downloads = 0;
            p.reserved_bytes = 0;
        });
    }
}

pub(super) async fn download_batch(
    s3: &S3Client,
    candidates: &[&MutationLogDownloadCandidate],
    urls: &[PresignedUrl],
    incorporated_frontier: &Frontier,
    progress: Option<&RestoreProgress>,
) -> SyncResult<(Vec<MutationLogSegment>, usize)> {
    let _queue_progress = QueueProgressGuard(progress);
    let mut downloads = FuturesOrdered::new();
    let mut reserved = 0u64;
    let mut next = 0;
    let mut downloaded = 0usize;
    let mut segments = Vec::with_capacity(candidates.len());
    while next < candidates.len() || !downloads.is_empty() {
        while next < candidates.len() && download_fits(downloads.len(), reserved, candidates[next])
        {
            let candidate = candidates[next];
            let url = &urls[next];
            reserved = reserved.saturating_add(candidate.listed_size as u64);
            downloads.push_back(async move {
                let result = progress::measure(
                    progress,
                    TransferOperation::Download,
                    s3.download_listed(url, candidate.listed_size),
                )
                .await;
                (candidate, result)
            });
            next += 1;
        }
        progress::update(progress, |p| {
            p.queued_downloads = downloads.len();
            p.reserved_bytes = reserved;
        });
        // Owned futures cancel on error or caller cancellation. Completed bodies
        // keep their reservation until this ordered queue consumes their result.
        let Some((candidate, result)) = downloads.next().await else {
            break;
        };
        let bytes = result?.ok_or_else(|| {
            SyncError::Storage(format!(
                "mutation-log segment {} missing during restore download",
                candidate.object_key
            ))
        })?;
        if bytes.len() != candidate.listed_size {
            return Err(SyncError::Storage(
                "mutation-log object differs from its listed size".into(),
            ));
        }
        downloaded = downloaded.saturating_add(1);
        reserved = reserved.saturating_sub(candidate.listed_size as u64);
        progress::update(progress, |p| {
            p.response_body_bytes = p.response_body_bytes.saturating_add(bytes.len() as u64);
            p.tail_objects_downloaded += 1;
            p.queued_downloads = downloads.len();
            p.reserved_bytes = reserved;
        });
        let writer = candidate
            .segment
            .writer_id
            .as_deref()
            .unwrap_or(&candidate.writer_hint);
        if !incorporated_frontier.covers_log(Some(writer), candidate.segment.through_id) {
            segments.push(MutationLogSegment {
                segment: candidate.segment.clone(),
                payload: bytes,
            });
        }
    }
    Ok((segments, downloaded))
}
