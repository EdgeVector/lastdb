use cloud_backup_protocol::v2::share_access::{ShareMemberRole, ShareMemberState};
use fold_db::canonical::CanonicalWriter;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{mutation::ContributionAdmission, Mutation, MutationType};
use mutation_log::Entry;
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

use super::{
    inbox, parse, resolve_schema, revision, terminal_record_error, CloudLog, ContributionError,
    ProposalResult, Submission, DEVICE_PREFIX, INBOX_NAMESPACE,
};
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use crate::named_grant::lock_applied;

/// Check a downloaded mutation against the publisher's confirmed sharing
/// settings and record rules. The receiver app's UI grants no write authority.
/// Commit an allowed change to the publisher's store for normal publication.
/// A terminal refusal consumes the pending entry without publishing a record.
pub(in crate::log_sync) async fn apply(
    host: &Host,
    log: &CloudLog,
    entry: &Entry,
) -> Result<ProposalResult, String> {
    use ProposalResult::{Discarded, Published};
    let Some(id) = entry
        .writer
        .device
        .as_str()
        .strip_prefix(DEVICE_PREFIX)
        .and_then(|value| cloud_backup_protocol::Id::try_from(value.to_owned()).ok())
        .filter(|_| {
            entry.writer_seq == 1
                && entry.incorporated_from.is_none()
                && entry.database_id.as_str() == log.database_id()
                && entry.namespace.as_str() == INBOX_NAMESPACE
        })
    else {
        return Ok(Discarded);
    };
    let database = log.database_id();
    let writer = entry.writer.key.as_str();
    let receipt = receipt_id(database, writer, id.as_str());
    let (store, committed) = load_receipt(host, database, &receipt).await?;
    if committed {
        return Ok(Published);
    }
    let (lock, applied) = lock_applied(&host.home, database)
        .await
        .map_err(|_| "member configuration is unavailable")?;
    let Some(member) = applied.update.as_ref().and_then(|update| {
        update.members.iter().find(|member| {
            member.node_public_key.as_str() == writer
                && member.state == ShareMemberState::Active
                && member.role == ShareMemberRole::Submit
        })
    }) else {
        return Ok(Discarded);
    };
    let Some(scope) = &member.write_schemas else {
        return Ok(Discarded);
    };
    let aad = inbox::context(
        database,
        writer,
        id.as_str(),
        entry.namespace.as_str(),
        entry.authored_at_ms,
    );
    let Ok(bytes) = inbox::open(&host.keypair, database, &aad, &entry.payload) else {
        return Ok(Discarded);
    };
    let Ok(submission) = serde_json::from_slice::<Submission>(&bytes) else {
        return Ok(Discarded);
    };
    if submission.grant_revision != applied.revision.get() {
        return Ok(Discarded);
    }
    let Ok(body) = serde_json::to_vec(&submission.operation) else {
        return Ok(Discarded);
    };
    let Ok(mut mutation) = parse(&body) else {
        return Ok(Discarded);
    };
    let (name, definition) = match resolve_schema(&store, &mutation) {
        Ok((name, definition)) if scope.permits(&name, &definition) => (name, definition),
        Err(ContributionError::Unavailable) => {
            return Err("member write schema is unavailable".into())
        }
        _ => return Ok(Discarded),
    };
    let base = submission.base_mutation.as_deref();
    if mutation.definition_id.as_deref() != Some(definition.as_str())
        || !revision::check_record_write(&store, &mut mutation, base, &name, writer).await?
    {
        return Ok(Discarded);
    }
    prepare(
        &mut mutation,
        receipt,
        writer,
        definition.as_str(),
        if base.is_some() { &bytes } else { &body },
    );
    let result = store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await;
    drop(lock);
    match result {
        Ok(_) => Ok(Published),
        Err(error) if terminal_record_error(&error) => Ok(Discarded),
        Err(_) => Err("member write could not be committed".into()),
    }
}

async fn load_receipt(
    host: &Host,
    database: &str,
    receipt: &[u8; 32],
) -> Result<(Arc<FoldDB>, bool), String> {
    let (store, _) = DatabaseContentKey::local_store(host, database)
        .map_err(|_| "member write store is unavailable")?
        .ok_or("member write store is unavailable")?;
    let committed = store
        .mutation_manager()
        .shared_contribution_committed(receipt)
        .await
        .map_err(|_| "member write receipt is unavailable")?;
    Ok((store, committed))
}

fn receipt_id(database: &str, writer: &str, request: &str) -> [u8; 32] {
    let encoded = CanonicalWriter::new()
        .field(b"lastdb:whole-database:submission-id:v1")
        .field(database.as_bytes())
        .field(writer.as_bytes())
        .field(request.as_bytes())
        .finish();
    Sha256::digest(encoded).into()
}

fn prepare(
    mutation: &mut Mutation,
    receipt_id: [u8; 32],
    writer: &str,
    definition: &str,
    body: &[u8],
) {
    mutation.uuid = uuid::Builder::from_random_bytes(
        receipt_id[..16]
            .try_into()
            .expect("receipt prefix is 16 bytes"),
    )
    .into_uuid()
    .to_string();
    mutation.pub_key = writer.to_owned();
    let creates_record = mutation.mutation_type == MutationType::Create;
    if creates_record {
        mutation
            .metadata
            .get_or_insert_with(std::collections::HashMap::new)
            .insert("share_author".into(), writer.to_owned());
    }
    mutation.contribution = Some(ContributionAdmission {
        definition_id: definition.to_owned(),
        receipt_id,
        intent_digest: Sha256::digest(body).into(),
        writer: writer.to_owned(),
        schema_scope: true,
        creates_record,
    });
    mutation.synchronous = Some(true);
}
