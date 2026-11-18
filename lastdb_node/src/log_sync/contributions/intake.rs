use cloud_backup_protocol::v2::share_access::ShareMemberRole;
use cloud_backup_protocol::Digest;
use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{
    cas::CasExpectation, operations::Operation, KeyValue, Mutation, MutationType, Query,
    SchemaError,
};
use mutation_log::Entry;
use std::sync::Arc;

mod commit;
mod decide;

use super::{
    inbox, schema_id, terminal_record_error, CloudLog, ContributionError, ProposalResult,
    Submission, BODY_LIMIT, DEVICE_PREFIX, INBOX_NAMESPACE,
};
use crate::host::share_identity::{self as identity, LocalIdentity};
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use crate::named_grant::lock_member;
use decide::{GrantView, SubmissionView};

pub(super) async fn receiver_base(
    host: &Host,
    database_id: &str,
    store: &FoldDB,
    mutation: &Mutation,
) -> Result<Option<String>, ContributionError> {
    let CasExpectation::Revision { revision } = &mutation.expected else {
        return Ok(None);
    };
    let found = match live_record(store, mutation).await {
        Ok(row) => row,
        Err(error) if terminal_record_error(&error) => return Ok(None),
        Err(_) => return Err(ContributionError::Unavailable),
    };
    let Some((current, _)) = found.filter(|(row, _)| row == revision) else {
        return Ok(None);
    };
    let address = store
        .mutation_manager()
        .record_storage_address(&mutation.schema_name, &mutation.key_value)
        .map_err(|_| ContributionError::Unavailable)?;
    let saved = identity::local_identity(&host.home, database_id, &address, &current)
        .map_err(|_| ContributionError::Unavailable)?;
    Ok(saved.map(|row| row.mutation_id))
}

pub(super) fn parse(body: &[u8]) -> Result<Mutation, ContributionError> {
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
        mutation_type,
        expected,
    );
    mutation.definition_id = definition_id;
    Ok(mutation)
}

pub(super) fn resolve_schema(
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

/// Check a downloaded mutation against the publisher's confirmed sharing
/// settings and record rules.
pub(in crate::log_sync) async fn apply(
    host: &Host,
    log: &CloudLog,
    entry: &Entry,
) -> Result<ProposalResult, String> {
    let Some(request) = submission_id(log, entry) else {
        return Ok(ProposalResult::Discarded);
    };
    let database = log.database_id();
    let writer = entry.writer.key.as_str();
    let receipt = commit::receipt_id(database, writer, &request);
    let store = publisher_store(host, database)?;
    if already_published(host, &store, database, &receipt)? {
        return Ok(ProposalResult::Published);
    }
    let Ok(node) = Digest::try_from(writer.to_owned()) else {
        return Ok(ProposalResult::Discarded);
    };
    let (lock, stored) = lock_member(&host.home, database, &node)
        .await
        .map_err(|_| "member configuration is unavailable")?;
    // The member, role, and scope come from the entry stored now. The
    // revision the submitter saw is not compared: a change to another
    // member's row must not discard this member's write.
    let grant = match stored.as_ref() {
        Some(member) if member.is_active() && member.role == ShareMemberRole::Submit => GrantView {
            active_submitter: true,
            scope: member.write_schemas.as_ref(),
        },
        _ => GrantView {
            active_submitter: false,
            scope: None,
        },
    };
    if !grant.active_submitter {
        return Ok(ProposalResult::Discarded);
    }
    let Some(opened) = open_submission(host, database, writer, &request, entry)? else {
        return Ok(ProposalResult::Discarded);
    };
    let (name, definition) = match resolve_schema(&store, &opened.mutation) {
        Ok(resolved) => resolved,
        Err(ContributionError::Unavailable) => {
            return Err("member write schema is unavailable".into())
        }
        Err(_) => return Ok(ProposalResult::Discarded),
    };
    let local = if opened.base.is_some() {
        load_local(host, &store, database, &opened.mutation).await?
    } else {
        None
    };
    let decision = decide::decide(
        &grant,
        &SubmissionView {
            schema_name: &name,
            mutation: &opened.mutation,
            definition: &definition,
            base: opened.base.as_deref(),
            writer,
            local: local.as_ref(),
        },
    );
    let result = commit::commit(
        commit::CommitInput {
            host,
            database,
            writer,
            receipt,
            definition: &definition,
            opened,
            decision,
        },
        &store,
    )
    .await?;
    drop(lock);
    Ok(result)
}

fn submission_id(log: &CloudLog, entry: &Entry) -> Option<String> {
    let value = entry.writer.device.as_str().strip_prefix(DEVICE_PREFIX)?;
    if entry.writer_seq != 1
        || entry.incorporated_from.is_some()
        || entry.database_id.as_str() != log.database_id()
        || entry.namespace.as_str() != INBOX_NAMESPACE
    {
        return None;
    }
    let id = cloud_backup_protocol::Id::try_from(value.to_owned()).ok()?;
    Some(id.as_str().to_owned())
}

fn already_published(
    host: &Host,
    store: &FoldDB,
    database: &str,
    receipt: &[u8; 32],
) -> Result<bool, String> {
    if identity::receipt_recorded(&host.home, database, receipt)? {
        return Ok(true);
    }
    let seen = store
        .db_ops()
        .native_runtime()
        .accepted_mutation_seen(&commit::mutation_uuid(receipt))
        .map_err(|_| "member write receipt is unavailable")?;
    if !seen {
        return Ok(false);
    }
    identity::store_receipt(&host.home, database, receipt)?;
    Ok(true)
}

fn publisher_store(host: &Host, database: &str) -> Result<Arc<FoldDB>, String> {
    DatabaseContentKey::local_store(host, database)
        .map_err(|_| "member write store is unavailable")?
        .map(|(store, _)| store)
        .ok_or_else(|| "member write store is unavailable".to_owned())
}

struct OpenedSubmission {
    mutation: Mutation,
    base: Option<String>,
    operation: Vec<u8>,
    sealed: Vec<u8>,
}

fn open_submission(
    host: &Host,
    database: &str,
    writer: &str,
    request: &str,
    entry: &Entry,
) -> Result<Option<OpenedSubmission>, String> {
    let aad = inbox::context(
        database,
        writer,
        request,
        entry.namespace.as_str(),
        entry.authored_at_ms,
    );
    let Ok(sealed) = inbox::open(&host.keypair, database, &aad, &entry.payload) else {
        return Ok(None);
    };
    let Ok(submission) = serde_json::from_slice::<Submission>(&sealed) else {
        return Ok(None);
    };
    let Ok(operation) = serde_json::to_vec(&submission.operation) else {
        return Ok(None);
    };
    let Ok(mutation) = parse(&operation) else {
        return Ok(None);
    };
    Ok(Some(OpenedSubmission {
        mutation,
        base: submission.base_mutation,
        operation,
        sealed,
    }))
}

async fn load_local(
    host: &Host,
    store: &FoldDB,
    database: &str,
    mutation: &Mutation,
) -> Result<Option<LocalIdentity>, String> {
    let found = match live_record(store, mutation).await {
        Ok(found) => found,
        Err(error) if terminal_record_error(&error) => return Ok(None),
        Err(_) => return Err("member write preimage is unavailable".into()),
    };
    let Some((revision, created_by)) = found else {
        return Ok(None);
    };
    let address = store
        .mutation_manager()
        .record_storage_address(&mutation.schema_name, &mutation.key_value)
        .map_err(|_| "member write preimage is unavailable")?;
    let Some(mut local) = identity::local_identity(&host.home, database, &address, &revision)?
    else {
        return Ok(None);
    };
    local.created_by = created_by;
    Ok(Some(local))
}

async fn live_record(
    store: &FoldDB,
    mutation: &Mutation,
) -> Result<Option<(String, Option<String>)>, SchemaError> {
    let query = Query::new_with_filter(
        mutation.schema_name.clone(),
        Some(key_filter(&mutation.key_value)?),
    );
    let records = store.query_executor().query(query).await?;
    if records.len() > 1 {
        return Err(SchemaError::InvalidData(
            "record lookup returned more than one row".into(),
        ));
    }
    Ok(records
        .into_values()
        .next()
        .map(|record| (record.revision, record.created_by)))
}

fn key_filter(key: &KeyValue) -> Result<HashRangeFilter, SchemaError> {
    let hash = key
        .hash
        .clone()
        .filter(|hash| !hash.is_empty())
        .ok_or_else(|| SchemaError::InvalidData("a record key requires a hash".into()))?;
    match &key.range {
        Some(range) if !range.is_empty() => Ok(HashRangeFilter::HashRangeKey {
            hash,
            range: range.clone(),
        }),
        _ => Ok(HashRangeFilter::HashKey(hash)),
    }
}
