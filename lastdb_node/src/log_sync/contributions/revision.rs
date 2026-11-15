//! Translate conditional write tokens between two copies of a published record.

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{cas::CasExpectation, Mutation};

use super::ContributionError;

pub(super) async fn receiver_base(
    store: &FoldDB,
    mutation: &Mutation,
) -> Result<Option<String>, ContributionError> {
    let CasExpectation::Revision { revision } = &mutation.expected else {
        return Ok(None);
    };
    let found = store
        .mutation_manager()
        .shared_record_revision(&mutation.schema_name, &mutation.key_value)
        .await
        .map_err(|_| ContributionError::Unavailable)?;
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
    let current = store
        .mutation_manager()
        .shared_record_revision(&mutation.schema_name, &mutation.key_value)
        .await
        .map_err(|_| "member write preimage is unavailable")?;
    let Some(current) = current.filter(|row| row.mutation_id == base) else {
        return Ok(false);
    };
    mutation.expected = CasExpectation::Revision {
        revision: current.local_revision,
    };
    Ok(true)
}
