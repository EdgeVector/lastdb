//! Selected-device job. Each step reconciles the same operation and source.

use super::protocol::{CloudPurgePhase, CloudReplacementManifest};
use super::{CloudPurgeJob, CloudPurgeJobPhase, RetainedSource};
use crate::sync::auth::AuthClient;
use crate::sync::error::{SyncError, SyncResult};
use async_trait::async_trait;
use rand::RngCore;
use std::sync::Arc;
use zeroize::Zeroizing;

mod cancellation;
mod root_registration;

#[async_trait]
pub trait CloudPurgeCapabilityStore: Send + Sync {
    async fn put(&self, operation_id: &str, value: &str) -> Result<String, String>;
    async fn get(&self, locator: &str) -> Result<Zeroizing<String>, String>;
}

pub struct CloudPurgeDriver {
    auth: AuthClient,
    secrets: Arc<dyn CloudPurgeCapabilityStore>,
}

impl CloudPurgeDriver {
    pub fn new(auth: AuthClient, secrets: Arc<dyn CloudPurgeCapabilityStore>) -> Self {
        Self { auth, secrets }
    }

    pub async fn begin(&self, job: &mut CloudPurgeJob, source: &RetainedSource) -> SyncResult<()> {
        if job.cancel_requested || job.phase == CloudPurgeJobPhase::Cancelled {
            return Err(SyncError::Storage(
                "this operation retains a durable cancellation intent".into(),
            ));
        }
        if source.local_only {
            return Err(SyncError::Storage(
                "private local recovery sources cannot enter cloud purge".into(),
            ));
        }
        source.verify()?;
        require_job_source(job, source)?;
        let preparation = self.auth.cloud_purge_prepare(&job.account).await?;
        if recover_accepted_begin(job, &preparation)? {
            return Ok(());
        }
        let preparation = self
            .register_source_roots(job, source, &preparation)
            .await?;
        super::preflight::admit_job(&preparation, job, source)?;
        if job.capability_locator.is_none() {
            let mut bytes = Zeroizing::new([0u8; 32]);
            rand::thread_rng().fill_bytes(&mut *bytes);
            let capability = Zeroizing::new(crate::hex::hex_lower(&bytes));
            let locator = self
                .secrets
                .put(&job.operation_id, &capability)
                .await
                .map_err(SyncError::Storage)?;
            job.capability_locator = Some(locator);
            job.save()?;
        }
        let capability = self.capability(job).await?;
        let accepted = self
            .auth
            .cloud_purge_begin(
                &job.account,
                &job.operation_id,
                job.expected_scope_version,
                &source.prepared,
                &capability,
            )
            .await?;
        job.update_control(accepted.control)
    }

    pub async fn status(&self, job: &mut CloudPurgeJob) -> SyncResult<()> {
        if job.control.is_none() {
            return Ok(());
        }
        let control = self
            .auth
            .cloud_purge_status(&job.account, &job.operation_id)
            .await?;
        job.update_control(control)
    }

    pub async fn status_readonly(&self, job: &mut CloudPurgeJob) -> SyncResult<()> {
        if job.control.is_none() {
            return Ok(());
        }
        let control = self
            .auth
            .cloud_purge_status(&job.account, &job.operation_id)
            .await?;
        job.observe_control(control)
    }

    /// One bounded service or object batch. Disconnect never changes the saved cut.
    pub async fn step(&self, job: &mut CloudPurgeJob, source: &RetainedSource) -> SyncResult<bool> {
        require_job_source(job, source)?;
        if job.cancel_requested {
            self.cancel(job).await?;
            return Ok(true);
        }
        if job.control.is_none() {
            self.begin(job, source).await?;
        }
        self.status(job).await?;
        let capability = self.capability(job).await?;
        let control = job
            .control
            .clone()
            .ok_or_else(|| SyncError::Storage("purge control absent".into()))?;
        match control.phase {
            CloudPurgePhase::Ready => {
                require_ready(job, source)?;
                Ok(true)
            }
            CloudPurgePhase::Cancelled => Ok(true),
            CloudPurgePhase::Uploading => {
                let complete =
                    super::upload::upload_next_batch(&self.auth, job, source, &capability).await?;
                if complete {
                    job.replacement = Some(make_replacement(job, source)?);
                    job.phase = CloudPurgeJobPhase::Publishing;
                    job.save()?;
                    let ready = self
                        .auth
                        .cloud_purge_commit(
                            job.replacement
                                .as_ref()
                                .ok_or_else(|| SyncError::Storage("replacement absent".into()))?,
                            &capability,
                        )
                        .await?;
                    job.update_control(ready)?;
                    if job.phase == CloudPurgeJobPhase::Ready {
                        require_ready(job, source)?;
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            CloudPurgePhase::Publishing => {
                let replacement = job
                    .replacement
                    .as_ref()
                    .ok_or_else(|| SyncError::Storage("saved publish intent absent".into()))?;
                let ready = self
                    .auth
                    .cloud_purge_commit(replacement, &capability)
                    .await?;
                job.update_control(ready)?;
                if job.phase == CloudPurgeJobPhase::Ready {
                    require_ready(job, source)?;
                }
                Ok(job.phase == CloudPurgeJobPhase::Ready)
            }
            _ => {
                let resumed = self
                    .auth
                    .cloud_purge_resume(
                        &job.account,
                        &job.operation_id,
                        control.generation,
                        &capability,
                    )
                    .await?;
                job.update_control(resumed)?;
                Ok(false)
            }
        }
    }

    async fn capability(&self, job: &CloudPurgeJob) -> SyncResult<Zeroizing<String>> {
        let locator = job.capability_locator.as_ref().ok_or_else(|| {
            SyncError::Storage("selected-device capability locator absent".into())
        })?;
        let value = self
            .secrets
            .get(locator)
            .await
            .map_err(SyncError::Storage)?;
        if !crate::hex::is_lower_hex_sha256(&value) {
            return Err(SyncError::Storage(
                "selected-device capability has an invalid shape".into(),
            ));
        }
        Ok(value)
    }
}

fn recover_accepted_begin(
    job: &mut CloudPurgeJob,
    preparation: &super::protocol::CloudPurgePreparation,
) -> SyncResult<bool> {
    let Some(control) = preparation
        .control
        .as_ref()
        .filter(|control| control["operation_id"].as_str() == Some(job.operation_id.as_str()))
    else {
        return Ok(false);
    };
    if job.capability_locator.is_none() {
        return Err(SyncError::Storage(
            "accepted purge has no saved selected capability".into(),
        ));
    }
    let control = serde_json::from_value(control.clone())
        .map_err(|_| SyncError::Storage("invalid accepted purge control".into()))?;
    job.update_control(control)?;
    Ok(true)
}

fn require_job_source(job: &CloudPurgeJob, source: &RetainedSource) -> SyncResult<()> {
    if source.account != job.account
        || source.prepared.source_id != job.source_id
        || source.prepared.manifest_sha256 != job.source_manifest_sha256
    {
        return Err(SyncError::Storage(
            "selected job source differs from its durable intent".into(),
        ));
    }
    Ok(())
}

fn make_replacement(
    job: &CloudPurgeJob,
    source: &RetainedSource,
) -> SyncResult<CloudReplacementManifest> {
    let control = job
        .control
        .as_ref()
        .ok_or_else(|| SyncError::Storage("control absent".into()))?;
    let removed_roots = control
        .scope_manifest
        .get("removed_roots")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            value.as_str().or_else(|| {
                value
                    .get("logical_root")
                    .and_then(serde_json::Value::as_str)
            })
        })
        .filter(|root| {
            !source
                .prepared
                .roots
                .iter()
                .any(|replacement| replacement.logical_root == *root)
        })
        .map(str::to_string)
        .collect();
    Ok(CloudReplacementManifest {
        format_version: 1,
        account: job.account.clone(),
        operation_id: job.operation_id.clone(),
        generation: control.generation,
        source_manifest_sha256: source.prepared.manifest_sha256.clone(),
        removed_roots,
        roots: source.prepared.roots.clone(),
    })
}

fn require_ready(job: &CloudPurgeJob, source: &RetainedSource) -> SyncResult<()> {
    let control = job
        .control
        .as_ref()
        .ok_or_else(|| SyncError::Storage("ready control absent".into()))?;
    let expected = make_replacement(job, source)?
        .sha256()
        .map_err(SyncError::Storage)?;
    let published = control
        .replacement_manifest
        .as_ref()
        .ok_or_else(|| SyncError::Storage("ready replacement absent".into()))?;
    let actual = published
        .get("sha256")
        .or_else(|| published.get("replacement_manifest_sha256"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            serde_json::from_value::<CloudReplacementManifest>(published.clone())
                .ok()
                .and_then(|manifest| manifest.sha256().ok())
        });
    if !control.deletion_is_complete() || actual.as_deref() != Some(expected.as_str()) {
        return Err(SyncError::Storage(
            "ready control lacks the exact deletion and replacement receipts".into(),
        ));
    }
    Ok(())
}
