//! Apply one accepted share decision and record its receipt.

use cloud_backup_protocol::Digest;
use fold_db::canonical::CanonicalWriter;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{cas::CasExpectation, mutation::ContributionAdmission, MutationType};
use sha2::{Digest as _, Sha256};

use super::super::{terminal_record_error, ProposalResult};
use super::{decide::IntakeDecision, OpenedSubmission};
use crate::host::share_identity as identity;
use crate::host::Host;

pub(super) struct CommitInput<'a> {
    pub(super) host: &'a Host,
    pub(super) database: &'a str,
    pub(super) writer: &'a str,
    pub(super) receipt: [u8; 32],
    pub(super) definition: &'a Digest,
    pub(super) opened: OpenedSubmission,
    pub(super) decision: IntakeDecision,
}

pub(super) async fn commit(
    input: CommitInput<'_>,
    store: &FoldDB,
) -> Result<ProposalResult, String> {
    let CommitInput {
        host,
        database,
        writer,
        receipt,
        definition,
        mut opened,
        decision,
    } = input;
    let (author_id, revision) = match decision {
        IntakeDecision::Publish {
            author_id,
            revision,
        } => (author_id, revision),
        IntakeDecision::Discard(reason) => {
            tracing::debug!(
                target: "lastdbd::log_sync",
                reason,
                "share submission discarded"
            );
            return Ok(ProposalResult::Discarded);
        }
    };
    if let Some(author) = author_id {
        opened
            .mutation
            .record_fields
            .insert("author_id".into(), author.into());
    }
    if let Some(revision) = revision {
        opened.mutation.expected = CasExpectation::Revision { revision };
    }
    let body: &[u8] = if opened.base.is_some() {
        &opened.sealed
    } else {
        &opened.operation
    };
    prepare(
        &mut opened.mutation,
        receipt,
        writer,
        definition.as_str(),
        body,
    );
    match store
        .mutation_manager()
        .write_mutations_receipt(vec![opened.mutation])
        .await
    {
        Ok(_) => {
            identity::store_receipt(&host.home, database, &receipt)?;
            Ok(ProposalResult::Published)
        }
        Err(error) if terminal_record_error(&error) => Ok(ProposalResult::Discarded),
        Err(_) => Err("member write could not be committed".into()),
    }
}

pub(super) fn receipt_id(database: &str, writer: &str, request: &str) -> [u8; 32] {
    let encoded = CanonicalWriter::new()
        .field(b"lastdb:whole-database:submission-id:v1")
        .field(database.as_bytes())
        .field(writer.as_bytes())
        .field(request.as_bytes())
        .finish();
    Sha256::digest(encoded).into()
}

pub(super) fn mutation_uuid(receipt: &[u8; 32]) -> String {
    uuid::Builder::from_random_bytes(
        receipt[..16]
            .try_into()
            .expect("receipt prefix is 16 bytes"),
    )
    .into_uuid()
    .to_string()
}

fn prepare(
    mutation: &mut fold_db::schema::types::Mutation,
    receipt_id: [u8; 32],
    writer: &str,
    definition: &str,
    body: &[u8],
) {
    mutation.uuid = mutation_uuid(&receipt_id);
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
