//! Upload parts are invisible until the cloud service admits completion.

use super::checkpoint::object_identity;
use super::protocol::{
    CloudPurgeObject, CloudPurgePartReceipt, CloudPurgeUpload, CloudPurgeUploadBatch,
    CloudPurgeUploadPart, CloudPurgeUploadReceipt,
};
use super::{CloudPurgeJob, RetainedSource};
use crate::sync::auth::AuthClient;
use crate::sync::error::{SyncError, SyncResult};
use futures::{stream, StreamExt};
use std::collections::BTreeSet;

pub(super) async fn upload_next_batch(
    auth: &AuthClient,
    job: &mut CloudPurgeJob,
    source: &RetainedSource,
    capability: &str,
) -> SyncResult<bool> {
    let control = job
        .control
        .as_ref()
        .ok_or_else(|| error("cloud control absent"))?;
    if !control.deletion_is_complete() {
        return Err(error("upload requires a complete cloud deletion receipt"));
    }
    let generation = control.generation;
    let objects = pending_grant_batch(job, source)?;
    if objects.is_empty() {
        return Ok(true);
    }
    let response = auth
        .cloud_purge_upload_batch(
            &job.account,
            &job.operation_id,
            generation,
            capability,
            &objects,
            &[],
        )
        .await?;
    require_response_coverage(&objects, &response)?;
    let receipts = upload_grants(source, response.uploads).await?;
    let verified = response
        .already_present
        .into_iter()
        .chain(response.verified)
        .collect::<Vec<_>>();
    for object in verified {
        job.uploaded_objects.insert(object_identity(&object));
    }
    if !receipts.is_empty() {
        let receipt_objects: Vec<_> = receipts
            .iter()
            .map(|receipt| receipt.object.clone())
            .collect();
        let confirmed = auth
            .cloud_purge_upload_batch(
                &job.account,
                &job.operation_id,
                generation,
                capability,
                &[],
                &receipts,
            )
            .await?;
        require_response_coverage(&receipt_objects, &confirmed)?;
        if !confirmed.uploads.is_empty() {
            return Err(error("cloud completion returned a new upload grant"));
        }
        for object in confirmed
            .verified
            .into_iter()
            .chain(confirmed.already_present)
        {
            job.uploaded_objects.insert(object_identity(&object));
        }
    }
    job.updated_at_unix_secs = crate::clock::unix_secs();
    job.save()?;
    Ok(source
        .prepared
        .objects
        .iter()
        .all(|object| job.uploaded_objects.contains(&object_identity(object))))
}

fn pending_grant_batch(
    job: &CloudPurgeJob,
    source: &RetainedSource,
) -> SyncResult<Vec<CloudPurgeObject>> {
    let mut objects = Vec::new();
    let mut part_count = 0u64;
    let mut estimated_bytes = 0u64;
    for object in &source.prepared.objects {
        if job.uploaded_objects.contains(&object_identity(object)) {
            continue;
        }
        let parts = object.bytes.div_ceil(5 * 1024 * 1024 * 1024).max(1);
        let estimate = parts.saturating_mul(2048).saturating_add(1024);
        if parts > 1000 || estimate > 3 * 1024 * 1024 {
            return Err(error(
                "source upload grant exceeds its bounded provider response",
            ));
        }
        if objects.len() == 32
            || part_count + parts > 1000
            || estimated_bytes + estimate > 3 * 1024 * 1024
        {
            break;
        }
        part_count += parts;
        estimated_bytes += estimate;
        objects.push(object.clone());
    }
    Ok(objects)
}

async fn upload_grants(
    source: &RetainedSource,
    uploads: Vec<CloudPurgeUpload>,
) -> SyncResult<Vec<CloudPurgeUploadReceipt>> {
    let results = stream::iter(uploads.into_iter().map(|upload| async move {
        let path = source.file_path(&upload.object)?;
        super::files::verify_file(&path, upload.object.bytes, &upload.object.sha256)?;
        tokio::task::spawn_blocking(move || upload_object(&path, upload))
            .await
            .map_err(|_| error("source upload worker failed"))?
    }))
    .buffer_unordered(4)
    .collect::<Vec<_>>()
    .await;
    Ok(results
        .into_iter()
        .collect::<SyncResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect())
}

fn require_response_coverage(
    requested: &[CloudPurgeObject],
    response: &CloudPurgeUploadBatch,
) -> SyncResult<()> {
    let mut covered = BTreeSet::new();
    for object in response
        .uploads
        .iter()
        .map(|upload| &upload.object)
        .chain(&response.already_present)
        .chain(&response.verified)
        .chain(&response.verification_pending)
    {
        if !requested.contains(object) || !covered.insert(object_identity(object)) {
            return Err(error(
                "cloud upload response changes or duplicates the retained object identity",
            ));
        }
    }
    if covered.len() != requested.len() {
        return Err(error(
            "cloud upload response omits requested retained objects",
        ));
    }
    Ok(())
}

fn upload_object(
    path: &std::path::Path,
    mut upload: CloudPurgeUpload,
) -> SyncResult<Option<CloudPurgeUploadReceipt>> {
    if upload.method != "PUT" || upload.upload_id.is_empty() || upload.completion_ticket.is_empty()
    {
        return Err(error("cloud did not supply a service-finalized upload"));
    }
    if upload.parts.is_empty() {
        upload.parts.push(CloudPurgeUploadPart {
            part_number: upload.part_number,
            url: std::mem::take(&mut upload.url),
            offset: 0,
            bytes: upload.object.bytes,
            headers: std::mem::take(&mut upload.headers),
        });
    }
    validate_parts(&upload.parts, upload.object.bytes)?;
    let mut receipts = completed_parts(&upload)?;
    if let Some(part) = upload.parts.iter().find(|part| {
        !receipts
            .iter()
            .any(|receipt| receipt.part_number == part.part_number)
    }) {
        receipts.push(CloudPurgePartReceipt {
            part_number: part.part_number,
            etag: super::upload_transport::upload_part(path, part)?,
        });
    }
    // Renew on the next worker step. Provider ListParts carries the completed
    // part proof across disconnect and restart without local private tickets.
    if receipts.len() != upload.parts.len() {
        return Ok(None);
    }
    receipts.sort_by_key(|receipt| receipt.part_number);
    let etag = receipts
        .first()
        .ok_or_else(|| error("source upload has no parts"))?
        .etag
        .clone();
    Ok(Some(CloudPurgeUploadReceipt {
        object: upload.object,
        upload_id: upload.upload_id,
        etag,
        completion_ticket: upload.completion_ticket,
        parts: receipts,
    }))
}

fn completed_parts(upload: &CloudPurgeUpload) -> SyncResult<Vec<CloudPurgePartReceipt>> {
    let mut numbers = BTreeSet::new();
    for receipt in &upload.completed_parts {
        if receipt.etag.is_empty()
            || !numbers.insert(receipt.part_number)
            || !upload
                .parts
                .iter()
                .any(|part| part.part_number == receipt.part_number)
        {
            return Err(error("source uploaded part proof differs from its grant"));
        }
    }
    Ok(upload.completed_parts.clone())
}

fn validate_parts(parts: &[CloudPurgeUploadPart], bytes: u64) -> SyncResult<()> {
    if parts.is_empty() || parts.len() > 10000 || bytes > 5_000_000_000_000 {
        return Err(error(
            "source upload exceeds provider object or part limits",
        ));
    }
    let mut offset = 0u64;
    for (index, part) in parts.iter().enumerate() {
        if part.part_number as usize != index + 1
            || part.offset != offset
            || part.bytes > 5 * 1024 * 1024 * 1024
            || (index + 1 < parts.len() && part.bytes < 5 * 1024 * 1024)
        {
            return Err(error(
                "source upload parts do not cover the retained object exactly",
            ));
        }
        offset = offset
            .checked_add(part.bytes)
            .ok_or_else(|| error("source upload part size overflow"))?;
    }
    if offset != bytes {
        return Err(error(
            "source upload part bytes differ from the retained object",
        ));
    }
    Ok(())
}

fn error(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
