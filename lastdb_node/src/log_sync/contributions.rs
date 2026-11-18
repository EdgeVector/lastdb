//! Queue encrypted member proposals on the proposal log.
//! Decided state: docs/designs/shared-database-mutation-logs.md (mutation log, proposal log, share settings).
//!
//! Receiver apps use schema grants to guide the UI. The publisher's intake
//! checks those grants against the actual mutation before any shared write.

mod intake;

use cloud_backup_protocol::v2::share_access::ShareWriteScope;
use cloud_backup_protocol::{Digest, Id};
use fold_db::schema::types::{Schema, SchemaError};
use mutation_log::{AppendBatch, AppendOutcome, DatabaseId, Entry, Namespace};
use serde::{Deserialize, Serialize};

use crate::accepted_writes;
use crate::host::owned::{Entry as OwnedEntry, ResolvedStore};
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use crate::named_grant::{ConfigError, ConfigUpdate};
use crate::registered_share::{self, RegisteredShare};
use crate::share_invitation::inbox;

use super::cloud::CloudLog;
use super::{
    http::{paths, Endpoint, Proof},
    received,
    session::retry,
    wire as log_wire,
};

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
    if update.writer_missing_scope() {
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

pub(crate) async fn submit(
    host: &Host,
    share: &RegisteredShare,
    id: &Id,
    body: &[u8],
) -> Result<u64, ContributionError> {
    use ContributionError::{Forbidden, Invalid, Unavailable};
    if !share.whole_database() {
        return Err(Forbidden);
    }
    let mutation = intake::parse(body)?;
    let store = registered_share::receiver_store(host, &share.id)
        .await
        .map_err(|_| Unavailable)?;
    let (_, definition) = intake::resolve_schema(&store, &mutation)?;
    let mut log = connection(host, share, id)?;
    let observed = received::access(&log).await.ok_or(Unavailable)?;
    let received::ObservedAccess::Active { revision, .. } = observed else {
        return Err(Forbidden);
    };
    log.member_revision = revision;
    let mut operation: serde_json::Value = serde_json::from_slice(body).map_err(|_| Invalid)?;
    operation["definition_id"] = serde_json::json!(definition);
    let submission = Submission {
        grant_revision: revision,
        operation,
        base_mutation: intake::receiver_base(host, share.id.as_str(), &store, &mutation).await?,
    };
    let invitation =
        registered_share::verified_invitation(&host.home, &share.id).map_err(|_| Unavailable)?;
    let key = invitation.inbox_key.ok_or(Unavailable)?;
    let request = seal_for_retry(host, &log, id, &submission, &key)?;
    let batch = AppendBatch {
        writer: log.reader(),
        entries: vec![request],
        accepted_at_ms: 0,
    };
    let body = log_wire::append_request(&batch).map_err(|_| Invalid)?;
    let reply = log
        .post(paths::APPEND, body)
        .await
        .map_err(|_| Unavailable)?;
    match log_wire::append_outcome(&reply, &batch) {
        Ok(AppendOutcome::Accepted { .. } | AppendOutcome::Duplicate { next_seq: 2 }) => Ok(1),
        _ => Err(Unavailable),
    }
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

fn seal_for_retry(
    host: &Host,
    log: &CloudLog,
    id: &Id,
    submission: &Submission,
    key: &x25519_dalek::PublicKey,
) -> Result<Entry, ContributionError> {
    use ContributionError::{Invalid, Unavailable};
    let name = format!("submission-{}", id.as_str());
    let database = log.database_id();
    let pending = host.home.join("log-sync").join(database);
    std::fs::create_dir_all(&pending).map_err(|_| Unavailable)?;
    let dir = crate::offline_home::Directory::open(&pending).map_err(|_| Unavailable)?;
    // This protects the exact ciphertext when two clients retry the same ID.
    let lock = dir
        .open_lock("member-submit.lock")
        .map_err(|_| Unavailable)?;
    lock.try_lock().map_err(|_| Unavailable)?;
    let operation =
        cloud_backup_protocol::canonical_bytes(&submission.operation).map_err(|_| Invalid)?;
    let digest = cloud_backup_protocol::bytes_sha256(&operation);
    let (at, saved) = match retry::load(&host.home, database, &name).map_err(|_| Unavailable)? {
        Some((seal, bytes)) if seal.namespace == digest.as_str() => (seal.authored_at_ms, bytes),
        Some(_) => return Err(Invalid),
        None => {
            let canonical =
                cloud_backup_protocol::canonical_bytes(submission).map_err(|_| Invalid)?;
            let at = fold_db::clock::unix_millis();
            let aad = inbox::context(
                database,
                log.principal().as_str(),
                id.as_str(),
                INBOX_NAMESPACE,
                at,
            );
            let bytes = inbox::seal(key, &aad, &canonical).map_err(|_| Unavailable)?;
            let seal = retry::Seal {
                writer_seq: 1,
                authored_at_ms: at,
                namespace: digest.as_str().to_owned(),
            };
            retry::store(&host.home, database, &name, &seal, &bytes).map_err(|_| Unavailable)?;
            (at, bytes)
        }
    };
    Ok(Entry {
        database_id: DatabaseId::new(database).map_err(|_| Invalid)?,
        writer: log.reader(),
        writer_seq: 1,
        authored_at_ms: at,
        namespace: Namespace::new(INBOX_NAMESPACE).map_err(|_| Invalid)?,
        incorporated_from: None,
        payload: saved,
    })
}

pub(super) fn schema_id(schema: &Schema) -> Result<Digest, ContributionError> {
    super::schema_id(schema).map_err(|_| ContributionError::Invalid)
}
