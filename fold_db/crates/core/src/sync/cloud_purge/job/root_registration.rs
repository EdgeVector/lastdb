//! Root registration precedes the refreshed account scope and cloud hold.

use super::CloudPurgeDriver;
use crate::sync::cloud_purge::inventory::{bounded_json, INVENTORY_METADATA_MAX_BYTES};
use crate::sync::cloud_purge::protocol::{CloudPurgeControl, CloudPurgePreparation};
use crate::sync::cloud_purge::{CloudPurgeJob, RetainedSource};
use crate::sync::{SyncError, SyncResult};

impl CloudPurgeDriver {
    pub(super) async fn register_source_roots(
        &self,
        job: &mut CloudPurgeJob,
        source: &RetainedSource,
        preparation: &CloudPurgePreparation,
    ) -> SyncResult<CloudPurgePreparation> {
        preparation.validate_inventory_capabilities()?;
        bounded_json(source, INVENTORY_METADATA_MAX_BYTES)?;
        if preparation.account != job.account {
            return Err(SyncError::Storage("cloud source account differs".into()));
        }
        let generation = match preparation.control.as_ref() {
            None => 0,
            Some(value) => {
                let control: CloudPurgeControl = serde_json::from_value(value.clone())
                    .map_err(|_| SyncError::Storage("invalid pre-hold cloud control".into()))?;
                control.validate_identity().map_err(SyncError::Storage)?;
                if control.account != job.account || !control.permits_local_adoption() {
                    return Err(SyncError::Storage(
                        "cloud source registration is held".into(),
                    ));
                }
                control.generation
            }
        };
        let registered_scope = self
            .auth
            .cloud_source_root_register(
                &job.account,
                &job.operation_id,
                generation,
                &source.prepared,
            )
            .await?;
        let refreshed = self.auth.cloud_purge_prepare(&job.account).await?;
        if refreshed.account != job.account || refreshed.scope_version != registered_scope {
            return Err(SyncError::Storage(
                "cloud source scope changed after root registration".into(),
            ));
        }
        job.expected_scope_version = refreshed.scope_version;
        job.save()?;
        Ok(refreshed)
    }
}
