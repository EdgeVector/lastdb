//! Bind semantic admission to the exact durable vocabulary used by a commit.

use crate::{types::CanonicalField, DeclaredFieldRecord, SchemaServiceState};
use schema_types::{FoldDbError, FoldDbResult, Schema};
use std::collections::HashMap;

const MAX_ADMISSION_RETRIES: usize = 10;
const VOCABULARY_CONFLICT: &str = "canonical_vocabulary_write_conflict";

/// An admitted batch and the durable definitions against which it was checked.
/// Backends must verify this snapshot inside their atomic commit, including
/// every optimistic-concurrency retry. A refresh before an unguarded write
/// cannot close the race between two independently cached service instances.
#[derive(Clone)]
pub struct CanonicalVocabularyWrite {
    pub expected: HashMap<String, CanonicalField>,
    pub fields: Vec<(String, CanonicalField)>,
}

impl CanonicalVocabularyWrite {
    pub fn verify(&self, current: &HashMap<String, CanonicalField>) -> FoldDbResult<()> {
        if current != &self.expected {
            return Err(FoldDbError::Config(format!(
                "{VOCABULARY_CONFLICT}: durable definitions changed; repeat semantic admission"
            )));
        }
        Ok(())
    }
}

pub(crate) enum VocabularyCommit<'a> {
    Schemas(&'a [Schema]),
    Declaration {
        record: &'a DeclaredFieldRecord,
        expected: Option<&'a DeclaredFieldRecord>,
    },
}

impl SchemaServiceState {
    /// Recheck the batch against durable state and publish only after the
    /// backend commits that same state. Contention has a bounded retry budget.
    ///
    /// Only fields that durable state lacks are written. A field-only batch
    /// with nothing new writes nothing: every cold start offers the whole
    /// built-in seed, and rewriting that unchanged image from many instances
    /// at once exhausted the conditional-write retries and failed starts.
    pub(crate) async fn commit_vocabulary(
        &self,
        fields: &[(String, CanonicalField)],
        operation: VocabularyCommit<'_>,
    ) -> FoldDbResult<()> {
        let backend = self.storage.backend();
        for _ in 0..MAX_ADMISSION_RETRIES {
            let expected = backend.load_all_canonical_fields().await?;
            let checked = self.checked_vocabulary_batch_against(&expected, fields)?;
            let fields: Vec<_> = checked
                .iter()
                .filter(|(name, field)| expected.get(name) != Some(field))
                .cloned()
                .collect();
            if fields.is_empty()
                && matches!(operation, VocabularyCommit::Schemas(schemas) if schemas.is_empty())
            {
                return self.cache_committed_vocabulary(&checked);
            }
            let vocabulary = CanonicalVocabularyWrite { expected, fields };
            let result = match operation {
                VocabularyCommit::Schemas(schemas) => {
                    backend
                        .save_schema_definitions_if_vocabulary(schemas, &vocabulary)
                        .await
                }
                VocabularyCommit::Declaration { record, expected } => {
                    backend
                        .save_declared_field_definitions_if_vocabulary(
                            record,
                            &vocabulary,
                            expected,
                        )
                        .await
                }
            };
            match result {
                Ok(()) => return self.cache_committed_vocabulary(&checked),
                Err(FoldDbError::Config(message)) if message.starts_with(VOCABULARY_CONFLICT) => {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(FoldDbError::Config(format!(
            "{VOCABULARY_CONFLICT}: admission exceeded {MAX_ADMISSION_RETRIES} retries"
        )))
    }
}
