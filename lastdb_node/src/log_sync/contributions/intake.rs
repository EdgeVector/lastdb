use cloud_backup_protocol::v2::share_access::ShareMemberRole;
use cloud_backup_protocol::Digest;
use fold_db::canonical::CanonicalWriter;
use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{
    cas::CasExpectation, mutation::ContributionAdmission, operations::Operation, Mutation,
    MutationType,
};
use fold_db::STANDARD_COMMENT_SCHEMA;
use mutation_log::Entry;
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

use super::{
    inbox, schema_id, terminal_record_error, CloudLog, ContributionError, ProposalResult,
    Submission, BODY_LIMIT, DEVICE_PREFIX, INBOX_NAMESPACE,
};
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use crate::named_grant::lock_member;

pub(super) async fn receiver_base(
    store: &FoldDB,
    mutation: &Mutation,
) -> Result<Option<String>, ContributionError> {
    let CasExpectation::Revision { revision } = &mutation.expected else {
        return Ok(None);
    };
    let found = match store
        .mutation_manager()
        .shared_record_revision(&mutation.schema_name, &mutation.key_value)
        .await
    {
        Ok(row) => row,
        Err(error) if terminal_record_error(&error) => return Ok(None),
        Err(_) => return Err(ContributionError::Unavailable),
    };
    Ok(found
        .filter(|row| row.local_revision == *revision)
        .map(|row| row.mutation_id))
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
        mutation_type,
        expected,
    );
    mutation.definition_id = definition_id;
    mutation.source_file_name = source_file_name;
    mutation.metadata = metadata;
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
    let Ok(node) = Digest::try_from(writer.to_owned()) else {
        return Ok(Discarded);
    };
    // One signed entry, read under the member lock. The size of the member
    // set does not change what this check costs.
    let (lock, stored) = lock_member(&host.home, database, &node)
        .await
        .map_err(|_| "member configuration is unavailable")?;
    let Some(member) =
        stored.filter(|stored| stored.is_active() && stored.role == ShareMemberRole::Submit)
    else {
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
    // The member, role and scope above come from the entry stored
    // now, and the record checks below read the store as it is now. The
    // revision the submitter saw is not compared: a change to another
    // member's row must not discard this member's write.
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
        || !check_record_write(&store, &mut mutation, base, &name, writer).await?
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

async fn check_record_write(
    store: &FoldDB,
    mutation: &mut Mutation,
    base: Option<&str>,
    schema: &str,
    writer: &str,
) -> Result<bool, String> {
    let standard_comment = schema == STANDARD_COMMENT_SCHEMA;
    let required_creator = if standard_comment {
        match mutation.mutation_type {
            MutationType::Create => None,
            MutationType::Replace => Some(writer),
            _ => return Ok(false),
        }
    } else {
        None
    };
    if !bind_publisher_revision(store, mutation, base, required_creator).await? {
        return Ok(false);
    }
    if standard_comment {
        let author = fold_db::hex::sha256_hex(writer.as_bytes());
        mutation
            .record_fields
            .insert("author_id".into(), author.into());
    }
    Ok(true)
}

async fn bind_publisher_revision(
    store: &FoldDB,
    mutation: &mut Mutation,
    base: Option<&str>,
    required_creator: Option<&str>,
) -> Result<bool, String> {
    let Some(base) = base else {
        return Ok(required_creator.is_none());
    };
    if !matches!(&mutation.expected, CasExpectation::Revision { .. }) {
        return Ok(false);
    }
    let current = match store
        .mutation_manager()
        .shared_record_revision(&mutation.schema_name, &mutation.key_value)
        .await
    {
        Ok(row) => row,
        Err(error) if terminal_record_error(&error) => return Ok(false),
        Err(_) => return Err("member write preimage is unavailable".into()),
    };
    let Some(current) = current.filter(|row| {
        row.mutation_id == base
            && required_creator
                .is_none_or(|creator| row.comment_creator.as_deref() == Some(creator))
    }) else {
        return Ok(false);
    };
    mutation.expected = CasExpectation::Revision {
        revision: current.local_revision,
    };
    Ok(true)
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
}
