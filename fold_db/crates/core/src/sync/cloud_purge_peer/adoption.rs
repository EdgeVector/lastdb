//! Exact replacement adoption and the local-before-cloud acknowledgement order.

use super::{held, sha256, CloudPurgePeer};
use crate::sync::auth::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudPurgeControl, CloudPurgePhase};
use crate::sync::error::SyncResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

/// The host installs the replacement in an inactive destination, retains the
/// prior local home/queue, then commits a local switch journal. Returning a
/// receipt means the switch is durable, not merely that downloads finished.
pub type AdoptionHandler = Arc<
    dyn Fn(AdoptionPlan) -> Pin<Box<dyn Future<Output = SyncResult<AdoptionReceipt>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdoptionPlan {
    pub account: CloudAccount,
    pub operation_id: String,
    pub generation: u64,
    pub revision: u64,
    pub replacement_manifest_sha256: String,
    pub replacement_manifest: Value,
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<Value>,
}

impl AdoptionPlan {
    /// Bind a standalone Cloud Off reader to an exact validated control.
    /// This creates no local adoption receipt and grants no cloud mutation.
    pub fn for_readonly_control(control: &CloudPurgeControl, source_id: &str) -> SyncResult<Self> {
        Self::from_control(control, source_id)
    }

    pub(crate) fn read_data_generation(&self) -> SyncResult<u64> {
        self.continuation
            .as_ref()
            .map_or(Ok(self.generation), |continuation| {
                continuation
                    .get("data_generation")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| held("the readonly continuation has no physical generation"))
            })
    }

    pub(super) fn from_control(control: &CloudPurgeControl, source_id: &str) -> SyncResult<Self> {
        control.validate_identity().map_err(held)?;
        if let Some(continuation) = control.unchanged_continuation() {
            return Ok(Self {
                account: control.account.clone(),
                operation_id: control.operation_id.clone(),
                generation: control.generation,
                revision: control.revision,
                replacement_manifest_sha256: sha256(continuation)?,
                replacement_manifest: Value::Null,
                source_id: source_id.into(),
                continuation: Some(continuation.clone()),
            });
        }
        if control.phase != CloudPurgePhase::Ready || !control.deletion_is_complete() {
            return Err(held("cloud purge has no verified ready replacement"));
        }
        let manifest = control
            .replacement_manifest
            .as_ref()
            .ok_or_else(|| held("ready cloud control has no replacement manifest"))?;
        validate_manifest_identity(manifest, control)?;
        Ok(Self {
            account: control.account.clone(),
            operation_id: control.operation_id.clone(),
            generation: control.generation,
            revision: control.revision,
            replacement_manifest_sha256: sha256(manifest)?,
            replacement_manifest: manifest.clone(),
            source_id: source_id.into(),
            continuation: None,
        })
    }

    pub(super) fn matches_control(&self, control: &CloudPurgeControl) -> SyncResult<()> {
        let contents = self
            .continuation
            .as_ref()
            .unwrap_or(&self.replacement_manifest);
        if sha256(contents)? != self.replacement_manifest_sha256 {
            return Err(held("the local adoption plan contents changed"));
        }
        let exact = Self::from_control(control, &self.source_id)?;
        if exact.account != self.account
            || exact.operation_id != self.operation_id
            || exact.generation != self.generation
            || exact.replacement_manifest_sha256 != self.replacement_manifest_sha256
            || exact.continuation != self.continuation
        {
            return Err(held(
                "local adoption does not match the current cloud replacement",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdoptionReceipt {
    pub account: CloudAccount,
    pub operation_id: String,
    pub generation: u64,
    pub source_id: String,
    pub retained_directory: PathBuf,
    pub retention_sha256: String,
    pub installed_manifest_sha256: String,
    /// Hash of the host's committed local home or org reference switch journal.
    pub switch_receipt_sha256: String,
}

impl AdoptionReceipt {
    pub fn matches(&self, plan: &AdoptionPlan) -> bool {
        self.account == plan.account
            && self.operation_id == plan.operation_id
            && self.generation == plan.generation
            && self.source_id == plan.source_id
            && self.installed_manifest_sha256 == plan.replacement_manifest_sha256
    }

    pub(super) fn validate(&self, plan: &AdoptionPlan) -> SyncResult<()> {
        if !self.matches(plan)
            || !crate::hex::is_lower_hex_sha256(&self.retention_sha256)
            || !crate::hex::is_lower_hex_sha256(&self.switch_receipt_sha256)
        {
            return Err(held(
                "local cloud adoption receipt is incomplete or mismatched",
            ));
        }
        Ok(())
    }
}

impl CloudPurgePeer {
    pub fn prepare_adoption(&self, control: &CloudPurgeControl) -> SyncResult<AdoptionPlan> {
        let plan = AdoptionPlan::from_control(control, &self.source_id)?;
        let current = self
            .known_control(&plan.account)?
            .ok_or_else(|| held("cloud replacement was not observed durably"))?;
        effective_adoption_control(&current, &plan)?;
        Ok(plan)
    }

    pub fn pending_adoption(&self, account: &CloudAccount) -> SyncResult<Option<AdoptionPlan>> {
        let Some(control) = self.known_control(account)? else {
            return Ok(None);
        };
        if !control.permits_local_adoption() {
            return Ok(None);
        }
        let plan = self.prepare_adoption(&control)?;
        Ok(self.adoption_receipt(&plan)?.is_none().then_some(plan))
    }

    /// Serialize adoption across independent publishers. A process restart
    /// repeats only the cloud acknowledgement after the local receipt commits.
    pub async fn ensure_adopted(
        &self,
        auth: &AuthClient,
        control: &CloudPurgeControl,
    ) -> SyncResult<String> {
        let plan = self.prepare_adoption(control)?;
        if self.continuation_needs_previous_ready(control)? {
            let previous: CloudPurgeControl =
                serde_json::from_value(control.previous_ready_control.clone().ok_or_else(
                    || held("the continuation lacks its required prior ready control"),
                )?)?;
            // The boxed boundary permits the authenticated immutable historical
            // restore without replacing the durable current control.
            Box::pin(self.ensure_adopted(auth, &previous)).await?;
        }
        self.require_continuation_floor(control)?;
        if let Some(token) = self.adoption_token(&plan)? {
            return Ok(token);
        }
        let _turn = self.adoption_turn.lock().await;
        if let Some(token) = self.adoption_token(&plan)? {
            return Ok(token);
        }
        if self.adoption_receipt(&plan)?.is_none() {
            let handler = self
                .adoption_handler()?
                .ok_or_else(|| held("the local host has no cloud replacement adoption handler"))?;
            let receipt = handler(plan.clone()).await?;
            self.mark_local_adopted(&plan, receipt)?;
        }
        // No queued operation obtains a new-generation token before the host
        // confirms the durable switch and retention of the old queue.
        let token = if plan.continuation.is_some() {
            let floor_token =
                Box::pin(auth.cloud_previous_ready_token(control, &plan.source_id)).await?;
            Box::pin(auth.cloud_control_continue(
                &plan.account,
                plan.generation,
                &plan.replacement_manifest_sha256,
                &plan.source_id,
                floor_token.as_deref(),
            ))
            .await?
        } else {
            let current = self.known_control(&plan.account)?.ok_or_else(|| {
                held("the current cloud control disappeared before acknowledgement")
            })?;
            if current.generation == plan.generation {
                Box::pin(auth.cloud_control_adopt(
                    &plan.account,
                    plan.generation,
                    &plan.replacement_manifest_sha256,
                    &plan.source_id,
                ))
                .await?
            } else {
                Box::pin(auth.cloud_previous_ready_token(&current, &plan.source_id))
                    .await?
                    .ok_or_else(|| held("the historical replacement acknowledgement is absent"))?
            }
        };
        self.complete_adoption(&plan, token.clone())?;
        Ok(token)
    }
}

/// A historical restore is authorized only by the exact last Ready seed
/// embedded in the current unchanged-cloud continuation. The current marker
/// remains the authority; callers never observe or install an older control.
pub fn effective_adoption_control(
    current: &CloudPurgeControl,
    plan: &AdoptionPlan,
) -> SyncResult<CloudPurgeControl> {
    if plan.matches_control(current).is_ok() {
        return Ok(current.clone());
    }
    let continuation = current
        .unchanged_continuation()
        .ok_or_else(|| held("the requested historical adoption has no current continuation"))?;
    let floor = continuation["required_adopted_generation"]
        .as_u64()
        .ok_or_else(|| held("the continuation prior adoption floor is absent"))?;
    let previous: CloudPurgeControl = serde_json::from_value(
        current
            .previous_ready_control
            .clone()
            .ok_or_else(|| held("the continuation prior ready control is absent"))?,
    )?;
    if previous.account != current.account
        || previous.generation != floor
        || plan.continuation.is_some()
        || plan.generation >= current.generation
    {
        return Err(held(
            "the historical adoption does not match the continuation floor",
        ));
    }
    plan.matches_control(&previous)?;
    Ok(previous)
}

fn validate_manifest_identity(manifest: &Value, control: &CloudPurgeControl) -> SyncResult<()> {
    if manifest.get("format_version").and_then(Value::as_u64) != Some(1)
        || manifest.get("generation").and_then(Value::as_u64) != Some(control.generation)
        || manifest.get("operation_id").and_then(Value::as_str)
            != Some(control.operation_id.as_str())
        || serde_json::from_value::<CloudAccount>(
            manifest.get("account").cloned().unwrap_or(Value::Null),
        )? != control.account
        || !manifest.get("roots").is_some_and(Value::is_array)
        || !manifest.get("removed_roots").is_some_and(Value::is_array)
    {
        return Err(held("cloud replacement manifest identity is invalid"));
    }
    let roots = manifest["roots"].as_array().expect("array checked above");
    let mut root_names = std::collections::BTreeSet::new();
    for root in roots {
        let name = root
            .get("logical_root")
            .and_then(Value::as_str)
            .ok_or_else(|| held("cloud replacement root has no identity"))?;
        if name.is_empty()
            || name.contains('/')
            || !root_names.insert(name)
            || !root.get("objects").is_some_and(Value::is_array)
        {
            return Err(held("cloud replacement roots are invalid or duplicated"));
        }
        for object in root["objects"].as_array().expect("array checked above") {
            let object: crate::sync::cloud_purge::protocol::CloudPurgeObject =
                serde_json::from_value(object.clone())?;
            object.validate().map_err(held)?;
            if object.logical_root != name {
                return Err(held("cloud replacement object names another root"));
            }
        }
    }
    Ok(())
}
