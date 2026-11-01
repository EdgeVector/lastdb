//! Assessing a manifest retention against the catalog schema a declare resolved to.
//!
//! The assessment reads the node (who shares the records, what the policy
//! change would be, whether records exist) and writes nothing. A preview
//! reports it. A declare acts on it.

use super::retention::RetentionLayout;
use super::*;
use fold_db::schema::{
    ManifestRetentionPlan, ManifestRetentionStatus, RetentionSharing, RetentionSource,
    SchemaRetentionPolicy,
};
use serde_json::{json, Map};

pub(super) struct Assessment {
    pub(super) ttl_seconds: u64,
    pub(super) layout: RetentionLayout,
    pub(super) sharing: RetentionSharing,
    pub(super) plan: Result<ManifestRetentionPlan, String>,
    pub(super) existing_records_may_expire: bool,
}

pub(super) async fn assess(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
    ttl_seconds: u64,
    layout: RetentionLayout,
    predecessors: &[String],
) -> Result<Assessment, String> {
    let store = host.db.db_ops().schemas();
    let binding = declare.local_schema_id.as_str();
    let sharing = store
        .retention_sharing(binding, anchor, &declare.namespace, predecessors)
        .await
        .map_err(|e| e.to_string())?;
    let plan = store
        .plan_manifest_retention(binding, anchor, ttl_seconds)
        .await
        .map_err(|e| e.to_string());
    let new_policy = plan
        .as_ref()
        .is_ok_and(|plan| plan.status == ManifestRetentionStatus::Applied);
    let existing_records_may_expire =
        layout.range_keyed && new_policy && existing_data(host, binding, anchor).await?;
    Ok(Assessment {
        ttl_seconds,
        layout,
        sharing,
        plan,
        existing_records_may_expire,
    })
}

/// Whether the schema already holds a record. Probes the record space of the
/// schema with one bounded read: no scan.
async fn existing_data(host: &Host, binding: &str, anchor: &str) -> Result<bool, String> {
    let manager = host.db.schema_manager();
    for name in [anchor, binding] {
        let Some(schema) = manager
            .get_schema_metadata(name)
            .map_err(|e| format!("failed to read schema {name}: {e}"))?
        else {
            continue;
        };
        let holds = host
            .db
            .db_ops()
            .atoms()
            .schema_may_hold_records(&schema)
            .await
            .map_err(|error| format!("failed to probe the records of {name}: {error}"))?;
        if holds {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Assessment {
    pub(super) fn shortened(&self) -> bool {
        self.plan.as_ref().is_ok_and(|plan| plan.shortened)
    }

    pub(super) fn needs_confirmation(&self) -> bool {
        self.shortened() || self.existing_records_may_expire
    }

    pub(super) fn existing_warning(&self, binding: &str) -> String {
        format!(
            "existing_records_may_expire: {binding} already holds records. Every record whose \
             range key is older than {}s is deleted for good: at the next sweep for a Range \
             schema, and in every HashRange partition that receives a later write",
            self.ttl_seconds
        )
    }

    /// Why the TTL must be refused, or `None` when it may be applied.
    pub(super) fn refusal(
        &self,
        declare: &DirectDeclareProposal,
        anchor: &str,
        confirmed: bool,
    ) -> Option<String> {
        let binding = declare.local_schema_id.as_str();
        if let Some(reason) = self.sharing.refusal(binding, anchor, &declare.namespace) {
            return Some(reason);
        }
        if let Err(error) = &self.plan {
            return Some(error.clone());
        }
        if confirmed || !self.needs_confirmation() {
            return None;
        }
        let mut why = Vec::new();
        if self.existing_records_may_expire {
            why.push(self.existing_warning(binding));
        }
        if self.shortened() {
            why.push(format!(
                "the manifest TTL would be shortened to {}s, so records between the two TTLs \
                 expire",
                self.ttl_seconds
            ));
        }
        Some(format!(
            "a manifest TTL on {binding} needs confirmation. {}. Review what would expire, then \
             run `lastdb app check --sync --confirm-expire-existing`",
            why.join(". ")
        ))
    }

    /// `new`, `shortened`, `lengthened` or `unchanged`, against the manifest TTL running.
    pub(super) fn change(&self) -> &'static str {
        let Ok(plan) = &self.plan else {
            return "unknown";
        };
        match (plan.status, &plan.current) {
            (ManifestRetentionStatus::Unchanged, _) => "unchanged",
            (_, Some(current)) if current.source == RetentionSource::Manifest => {
                if self.ttl_seconds < current.ttl_seconds {
                    "shortened"
                } else {
                    "lengthened"
                }
            }
            _ => "new",
        }
    }

    pub(super) fn previous_ttl_seconds(&self) -> Option<u64> {
        self.plan
            .as_ref()
            .ok()
            .and_then(|plan| plan.current.as_ref())
            .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
    }

    /// The fields a preview and an apply report share.
    pub(super) fn report(
        &self,
        binding: &str,
        anchor: &str,
        confirmed: bool,
    ) -> Map<String, Value> {
        let mut warnings = self.layout.warnings.clone();
        if self.existing_records_may_expire {
            warnings.push(self.existing_warning(binding));
        }
        let mut report = Map::new();
        report.insert("ttl_seconds".into(), json!(self.ttl_seconds));
        report.insert("source".into(), json!(RetentionSource::Manifest.as_str()));
        report.insert("anchor".into(), json!(anchor));
        report.insert("change".into(), json!(self.change()));
        report.insert(
            "previous_ttl_seconds".into(),
            json!(self.previous_ttl_seconds()),
        );
        report.insert("age_rule".into(), json!(self.layout.age_rule));
        report.insert("warnings".into(), json!(warnings));
        report.insert("shared_with".into(), json!(self.sharing.shared_with));
        report.insert(
            "existing_records_may_expire".into(),
            json!(self.existing_records_may_expire),
        );
        report.insert("shortened".into(), json!(self.shortened()));
        report.insert("confirmed_expire_existing".into(), json!(confirmed));
        report
    }
}
