//! Chunk upload with retry for the S0 publisher. Moved verbatim from `cloud_rescue_publish.rs`.

use super::*;

pub(super) struct ChunkAttemptError {
    pub(super) message: String,
    pub(super) retryable: bool,
}

impl ChunkAttemptError {
    pub(super) fn sync(message: &str, error: &SyncError) -> Self {
        Self {
            message: format!("{message}: {error}"),
            retryable: matches!(error, SyncError::Network(_)),
        }
    }

    pub(super) fn fatal(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}

pub(super) async fn retry_chunk_attempt<F, Fut>(mut operation: F) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), ChunkAttemptError>>,
{
    for attempt in 1..=MAX_CHUNK_ATTEMPTS {
        match operation().await {
            Ok(()) => return Ok(()),
            Err(error) if error.retryable && attempt < MAX_CHUNK_ATTEMPTS => {
                let delay_ms = CHUNK_RETRY_BASE_DELAY_MS << (attempt - 1);
                eprintln!(
                    "S0 rescue chunk network retry {}/{} after {} ms",
                    attempt + 1,
                    MAX_CHUNK_ATTEMPTS,
                    delay_ms
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error.message),
        }
    }
    unreachable!("bounded chunk retry always returns")
}

pub(super) async fn upload_chunk(
    data_root: PathBuf,
    candidate: BackupChunkUploadCandidate,
    identity: RescueS0Identity,
    store_uuid: String,
    sha: String,
    auth: fold_db::sync::auth::AuthClient,
    s3: fold_db::sync::s3::S3Client,
) -> Result<(), String> {
    retry_chunk_attempt(|| {
        upload_chunk_once(
            &data_root,
            &candidate,
            &identity,
            &store_uuid,
            &sha,
            &auth,
            &s3,
        )
    })
    .await
}

pub(super) async fn upload_chunk_once(
    data_root: &Path,
    candidate: &BackupChunkUploadCandidate,
    identity: &RescueS0Identity,
    store_uuid: &str,
    sha: &str,
    auth: &fold_db::sync::auth::AuthClient,
    s3: &fold_db::sync::s3::S3Client,
) -> Result<(), ChunkAttemptError> {
    let bytes = candidate.chunk.bytes;
    let presign = auth
        .rescue_s0_presign_chunk_upload(identity, store_uuid, sha, bytes)
        .await
        .map_err(|error| ChunkAttemptError::sync("request S0 rescue chunk upload", &error))?;
    if !presign.already_present {
        let signed = presign.url.ok_or_else(|| {
            ChunkAttemptError::fatal("S0 rescue chunk upload URL is missing".into())
        })?;
        let data_root = data_root.to_path_buf();
        let candidate = candidate.clone();
        let file = tokio::task::spawn_blocking(move || checked_file(&data_root, &candidate))
            .await
            .map_err(|error| {
                ChunkAttemptError::fatal(format!("read S0 rescue chunk task: {error}"))
            })?
            .map_err(ChunkAttemptError::fatal)?;
        s3.upload_rescue_file(&signed, file, bytes)
            .await
            .map_err(|error| {
                ChunkAttemptError::sync(&format!("upload S0 rescue chunk {sha}"), &error)
            })?;
    }
    auth.rescue_s0_confirm_chunk(identity, store_uuid, sha, bytes)
        .await
        .map_err(|error| ChunkAttemptError::sync(&format!("confirm S0 rescue chunk {sha}"), &error))
}

pub(super) async fn upload_page_chunks(
    home: &Path,
    plan: &RescuePlan,
    prefix: &str,
    chunks: &[String],
    candidates: &BTreeMap<String, BackupChunkUploadCandidate>,
    auth: &fold_db::sync::auth::AuthClient,
    s3: &fold_db::sync::s3::S3Client,
) -> Result<(), String> {
    let marker_path = home.join(format!("{PAGE_FILE_PREFIX}{prefix}.json"));
    let expected_marker = page_marker(plan, prefix, chunks);
    if let Some(bytes) = read_small_regular(&marker_path, 4096)? {
        let marker: RescuePageMarker =
            serde_json::from_slice(&bytes).map_err(|_| "invalid local S0 rescue page marker")?;
        if marker != expected_marker {
            return Err("local S0 rescue page marker does not match the plan".into());
        }
        if auth
            .rescue_s0_verify_page(&plan.identity(), &plan.manifest.store_uuid, prefix, chunks)
            .await
            .is_ok()
        {
            return Ok(());
        }
    }

    let (small, large) = split_page_chunks(chunks, candidates)?;
    let mut pending = small.into_iter();
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        while tasks.len() < UPLOAD_CONCURRENCY {
            let Some(sha) = pending.next() else { break };
            let candidate = candidates
                .get(sha)
                .ok_or("S0 rescue page names a missing local chunk")?
                .clone();
            let sha = sha.clone();
            let identity = plan.identity();
            let store_uuid = plan.manifest.store_uuid.clone();
            let data_root = home.join("data");
            let auth = auth.clone();
            let s3 = s3.clone();
            tasks.spawn(upload_chunk(
                data_root, candidate, identity, store_uuid, sha, auth, s3,
            ));
        }
        let Some(outcome) = tasks.join_next().await else {
            break;
        };
        outcome.map_err(|error| format!("S0 rescue chunk task failed: {error}"))??;
    }
    // The task pool is empty before any large confirmation starts.
    for sha in large {
        let candidate = candidates
            .get(sha)
            .ok_or("S0 rescue page names a missing local chunk")?
            .clone();
        upload_chunk(
            home.join("data"),
            candidate,
            plan.identity(),
            plan.manifest.store_uuid.clone(),
            sha.clone(),
            auth.clone(),
            s3.clone(),
        )
        .await?;
    }
    auth.rescue_s0_verify_page(&plan.identity(), &plan.manifest.store_uuid, prefix, chunks)
        .await
        .map_err(|error| format!("verify S0 rescue page {prefix}: {error}"))?;
    if read_small_regular(&marker_path, 4096)?.is_none() {
        let bytes = serde_json::to_vec(&expected_marker)
            .map_err(|error| format!("encode S0 rescue page marker: {error}"))?;
        save_once(&marker_path, &bytes)?;
    }
    Ok(())
}
