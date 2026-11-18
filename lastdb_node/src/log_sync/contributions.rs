//! Queue encrypted mutations from active receivers in the publisher's pending log.
//!
//! Receiver apps use schema grants to guide the UI. The publisher's intake
//! checks those grants against the actual mutation before any shared write.

mod intake;

use cloud_backup_protocol::v2::share_access::ShareWriteScope;
use cloud_backup_protocol::{Digest, Id};
use fold_db::schema::types::{Schema, SchemaError};
use serde::{Deserialize, Serialize};

use crate::accepted_writes;
use crate::host::owned::{Entry as OwnedEntry, ResolvedStore};
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use crate::named_grant::{ConfigError, ConfigUpdate};
use crate::registered_share::RegisteredShare;

use super::cloud::CloudLog;
use super::http::{paths, Endpoint, Proof};

pub(super) use intake::apply;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ProposalResult {
    Published,
    Discarded,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShareWriteStatus {
    Submitted,
    Published,
    Discarded,
    Unknown,
}

#[derive(Clone, Copy)]
pub(crate) enum ContributionError {
    Invalid,
    Forbidden,
    Unavailable,
}

fn terminal_record_error(error: &SchemaError) -> bool {
    matches!(
        error,
        SchemaError::CasConflict { .. }
            | SchemaError::InvalidData(_)
            | SchemaError::InvalidField(_)
            | SchemaError::NotFound(_)
            | SchemaError::RecordTooLarge { .. }
    )
}

// The normal control request cap bounds both the encrypted envelope and JSON.
const BODY_LIMIT: usize = 96 * 1024;
const DEVICE_PREFIX: &str = "member-";
const INBOX_NAMESPACE: &str = "lastdb-member-submission";

pub(super) async fn publish_before_settlement(host: &Host, log: &CloudLog) -> Result<(), String> {
    let (store, _) = DatabaseContentKey::local_store(host, log.database_id())
        .map_err(|_| "inbox write store is unavailable")?
        .ok_or("inbox write store is unavailable")?;
    let runtime = store.db_ops().native_runtime();
    let through = accepted_writes::status(&runtime).head;
    if accepted_writes::status(&runtime).acked < through {
        super::accepted::publish_owned(host, log).await?;
    }
    if accepted_writes::status(&runtime).acked < through {
        return Err("accepted inbox writes still await publication".into());
    }
    Ok(())
}

pub(super) async fn settle_legacy(host: &Host, log: &CloudLog, through: u64) -> Result<(), String> {
    // A retry can repeat intake safely using the durable contribution receipts.
    publish_before_settlement(host, log).await?;
    let request = serde_json::json!({
        "database_id": log.database_id(),
        "through": cloud_backup_protocol::DecimalU64::new(through),
    });
    let bytes = cloud_backup_protocol::canonical_bytes(&request)
        .map_err(|_| "member settlement request was not encoded")?;
    log.post(paths::SETTLE_SUBMISSIONS, bytes)
        .await
        .map(|_| ())
        .map_err(|error| format!("member submissions were not settled: {error}"))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Submission {
    grant_revision: u64,
    operation: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_mutation: Option<String>,
}

pub(crate) fn validate_write_scopes(
    host: &Host,
    entry: &OwnedEntry,
    update: &ConfigUpdate,
) -> Result<(), ConfigError> {
    let invalid = |message: &str| ConfigError::Invalid(message.to_owned());
    if !entry.whole_database() && update.scoped_members().next().is_some() {
        return Err(invalid(
            "schema write scopes require whole-database sharing",
        ));
    }
    if !entry.whole_database() {
        return Ok(());
    }
    if update.has_legacy_writer() {
        return Err(invalid("every writer needs an explicit schema write scope"));
    }
    let ResolvedStore::Owned(store) = host
        .owned
        .registered_store(&entry.id)
        .map_err(|_| ConfigError::Unconfirmed)?
    else {
        return Err(ConfigError::Unconfirmed);
    };
    let schemas = store
        .schema_manager()
        .get_schemas()
        .map_err(|_| ConfigError::Unconfirmed)?;
    let installed: std::collections::HashSet<_> = schemas
        .values()
        .filter_map(|schema| schema_id(schema).ok())
        .collect();
    for scope in update.scoped_members() {
        match scope {
            ShareWriteScope::Selected(ids) if ids.iter().any(|id| !installed.contains(id)) => {
                return Err(invalid(
                    "write scope contains a schema definition outside this database",
                ));
            }
            ShareWriteScope::Named { names }
                if names.iter().any(|name| !schemas.contains_key(name)) =>
            {
                return Err(invalid(
                    "write scope contains a schema name outside this database",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) async fn status(
    host: &Host,
    share: &RegisteredShare,
    id: &Id,
) -> Result<ShareWriteStatus, ContributionError> {
    if !share.whole_database() {
        return Err(ContributionError::Forbidden);
    }
    let log = connection(host, share, id)?;
    let body = cloud_backup_protocol::canonical_bytes(&serde_json::json!({
        "database_id": log.database_id(), "mutation_id": id,
    }))
    .map_err(|_| ContributionError::Invalid)?;
    let (status, body) = log
        .post_status(paths::ACCESS, body, Proof::Data)
        .await
        .map_err(|_| ContributionError::Unavailable)?;
    if matches!(status, 401 | 403) {
        return Err(ContributionError::Forbidden);
    }
    if status != 200 {
        return Err(ContributionError::Unavailable);
    }
    let reply: serde_json::Value =
        serde_json::from_slice(&body).map_err(|_| ContributionError::Unavailable)?;
    // An older service returns its ordinary access response.
    if reply.get("mutation_id").is_none() {
        return Ok(ShareWriteStatus::Unknown);
    }
    if reply["mutation_id"].as_str() != Some(id.as_str()) {
        return Err(ContributionError::Unavailable);
    }
    serde_json::from_value(reply["status"].clone()).map_err(|_| ContributionError::Unavailable)
}

fn connection(
    host: &Host,
    share: &RegisteredShare,
    id: &Id,
) -> Result<CloudLog, ContributionError> {
    let (label, api_url) = received::profile_service(share.environment());
    let endpoint = Endpoint::open(api_url, label).map_err(|_| ContributionError::Unavailable)?;
    CloudLog::open_offline(
        endpoint,
        share.stream().scope.clone(),
        share.id.clone(),
        &format!("{DEVICE_PREFIX}{}", id.as_str()),
        host.keypair.clone(),
        label,
    )
    .map_err(|_| ContributionError::Unavailable)
}

pub(super) fn schema_id(schema: &Schema) -> Result<Digest, ContributionError> {
    super::schema_id(schema).map_err(|_| ContributionError::Invalid)
}
