//! Translate conditional write tokens between two copies of a published record.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{cas::CasExpectation, Mutation, MutationType};
use schema_types::STANDARD_COMMENT_SCHEMA;
use sha2::{Digest, Sha256};

use super::{terminal_record_error, ContributionError};

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

pub(super) async fn check_record_write(
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
        let author = hex::encode(Sha256::digest(writer.as_bytes()));
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
