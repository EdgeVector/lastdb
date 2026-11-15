//! Translate conditional write tokens between two copies of a published record.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{cas::CasExpectation, Mutation};

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

pub(super) async fn bind_publisher_revision(
    store: &FoldDB,
    mutation: &mut Mutation,
    base: Option<&str>,
) -> Result<bool, String> {
    let Some(base) = base else { return Ok(true) };
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
    let Some(current) = current.filter(|row| row.mutation_id == base) else {
        return Ok(false);
    };
    mutation.expected = CasExpectation::Revision {
        revision: current.local_revision,
    };
    Ok(true)
}
