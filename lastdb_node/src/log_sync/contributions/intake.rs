use cloud_backup_protocol::v2::share_access::{ShareMemberRole, ShareMemberState};
use fold_db::canonical::CanonicalWriter;
use fold_db::schema::types::{mutation::ContributionAdmission, Mutation};
use mutation_log::Entry;
use sha2::{Digest as _, Sha256};

use super::{
    inbox, parse, resolve_schema, revision, terminal_record_error, CloudLog, ContributionError,
    Submission, DEVICE_PREFIX, INBOX_NAMESPACE,
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
) -> Result<(), String> {
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
        return Ok(());
    };
    let database = log.database_id();
    let writer = entry.writer.key.as_str();
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
        return Ok(());
    };
    let Some(scope) = &member.write_schemas else {
        return Ok(());
    };
    let aad = inbox::context(
        database,
        writer,
        id.as_str(),
        entry.namespace.as_str(),
        entry.authored_at_ms,
    );
    let Ok(bytes) = inbox::open(&host.keypair, database, &aad, &entry.payload) else {
        return Ok(());
    };
    let Ok(submission) = serde_json::from_slice::<Submission>(&bytes) else {
        return Ok(());
    };
    if submission.grant_revision != applied.revision.get() {
        return Ok(());
    }
    let Ok(body) = serde_json::to_vec(&submission.operation) else {
        return Ok(());
    };
    let Ok(mut mutation) = parse(&body) else {
        return Ok(());
    };
    let (store, _) = DatabaseContentKey::local_store(host, database)
        .map_err(|_| "member write store is unavailable")?
        .ok_or("member write store is unavailable")?;
    let (name, definition) = match resolve_schema(&store, &mutation) {
        Ok((name, definition)) if scope.permits(&name, &definition) => (name, definition),
        Err(ContributionError::Unavailable) => {
            return Err("member write schema is unavailable".into())
        }
        _ => return Ok(()),
    };
    let base = submission.base_mutation.as_deref();
    if mutation.definition_id.as_deref() != Some(definition.as_str())
        || !revision::check_record_write(&store, &mut mutation, base, &name, writer).await?
    {
        return Ok(());
    }
    prepare(
        &mut mutation,
        database,
        writer,
        id.as_str(),
        definition.as_str(),
        if base.is_some() { &bytes } else { &body },
    );
    let result = store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await;
    drop(lock);
    match result {
        Ok(_) => Ok(()),
        Err(error) if terminal_record_error(&error) => Ok(()),
        Err(_) => Err("member write could not be committed".into()),
    }
}

fn prepare(
    mutation: &mut Mutation,
    database: &str,
    writer: &str,
    request: &str,
    definition: &str,
    body: &[u8],
) {
    let encoded = CanonicalWriter::new()
        .field(b"lastdb:whole-database:submission-id:v1")
        .field(database.as_bytes())
        .field(writer.as_bytes())
        .field(request.as_bytes())
        .finish();
    let receipt_id: [u8; 32] = Sha256::digest(encoded).into();
    mutation.uuid = uuid::Builder::from_random_bytes(
        receipt_id[..16]
            .try_into()
            .expect("receipt prefix is 16 bytes"),
    )
    .into_uuid()
    .to_string();
    mutation.pub_key = writer.to_owned();
    if matches!(
        mutation.mutation_type,
        fold_db::schema::types::MutationType::Create
    ) {
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
    });
    mutation.synchronous = Some(true);
}
