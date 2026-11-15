//! Queue encrypted mutations from active receivers in the publisher's pending log.
//!
//! Receiver apps use schema grants to guide the UI. The publisher's intake
//! checks those grants against the actual mutation before any shared write.

mod intake;

use cloud_backup_protocol::v2::share_access::ShareWriteScope;
use cloud_backup_protocol::{Digest, Id};
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{Mutation, MutationType, Schema};
use mutation_log::{AppendBatch, AppendOutcome, DatabaseId, Entry, Namespace};
use serde::{Deserialize, Serialize};

use crate::host::owned::{Entry as OwnedEntry, Resolved};
use crate::host::Host;
use crate::named_grant::{ConfigError, ConfigUpdate};
use crate::registered_share::{self, RegisteredShare};
use crate::share_invitation::inbox;

use super::cloud::CloudLog;
use super::{http::Endpoint, members, paths, received, wire as log_wire};

pub(super) use intake::apply;

#[derive(Clone, Copy)]
pub(crate) enum ContributionError {
    Invalid,
    Forbidden,
    Unavailable,
}

// The normal control request cap bounds both the encrypted envelope and JSON.
const BODY_LIMIT: usize = 96 * 1024;
const DEVICE_PREFIX: &str = "member-";
const INBOX_NAMESPACE: &str = "lastdb-member-submission";

pub(super) async fn settle(log: &CloudLog, through: u64) -> Result<(), String> {
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
    let (Resolved::Owned, Some(store)) = host
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
    let mutation = parse(body)?;
    let store = registered_share::receiver_store(host, &share.id)
        .await
        .map_err(|_| Unavailable)?;
    let (_, definition) = resolve_schema(&store, &mutation)?;
    let mut log = connection(host, share, id)?;
    let observed = members::access(&log).await.ok_or(Unavailable)?;
    let members::ObservedAccess::Active { revision, .. } = observed else {
        return Err(Forbidden);
    };
    log.member_revision = revision;
    let mut operation: serde_json::Value = serde_json::from_slice(body).map_err(|_| Invalid)?;
    operation["definition_id"] = serde_json::json!(definition);
    let submission = Submission {
        grant_revision: revision,
        operation,
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
    let (at, saved) = match super::retry::load(&host.home, database, &name)
        .map_err(|_| Unavailable)?
    {
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
            let seal = super::retry::Seal {
                writer_seq: 1,
                authored_at_ms: at,
                namespace: digest.as_str().to_owned(),
            };
            super::retry::store(&host.home, database, &name, &seal, &bytes)
                .map_err(|_| Unavailable)?;
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

fn parse(body: &[u8]) -> Result<Mutation, ContributionError> {
    use fold_db::record_replacement::RecordTarget;
    use fold_db::schema::types::operations::Operation;
    if body.len() > BODY_LIMIT {
        return Err(ContributionError::Invalid);
    }
    let Operation::Mutation {
        schema,
        record_fields,
        key_value,
        alternates,
        mutation_type,
        expected,
        definition_id,
        source_file_name,
        metadata,
        file,
        ..
    } = serde_json::from_slice(body).map_err(|_| ContributionError::Invalid)?;
    if mutation_type == MutationType::Purge || file.is_some() {
        return Err(ContributionError::Forbidden);
    }
    let target = RecordTarget {
        schema,
        key: key_value,
        alternates,
    };
    let mut mutation = Mutation::with_uuid(
        String::new(),
        target,
        record_fields,
        String::new(),
        mutation_type,
        expected,
    );
    mutation.definition_id = definition_id;
    mutation.source_file_name = source_file_name;
    mutation.metadata = metadata;
    Ok(mutation)
}

fn resolve_schema(
    store: &FoldDB,
    mutation: &Mutation,
) -> Result<(String, Digest), ContributionError> {
    let schema = store
        .schema_manager()
        .get_schema_metadata(&mutation.schema_name)
        .map_err(|_| ContributionError::Unavailable)?
        .ok_or(ContributionError::Invalid)?;
    let definition = schema_id(&schema)?;
    Ok((schema.name.clone(), definition))
}

pub(crate) fn schema_id(schema: &Schema) -> Result<Digest, ContributionError> {
    let id = schema
        .identity_hash
        .as_deref()
        .ok_or(ContributionError::Invalid)?;
    let portable = schema_types::PortableSchemaId::from_catalog(id, schema.to_wire())
        .map_err(|_| ContributionError::Invalid)?;
    Digest::try_from(portable.as_str().to_owned()).map_err(|_| ContributionError::Invalid)
}
