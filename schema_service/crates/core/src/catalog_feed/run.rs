//! One feed run: lists, manifests, records, vocabulary, then one conditional
//! image write. Nothing partial is published: a failed run leaves the stored
//! image as it was, and the next run starts again from it.

use std::collections::{HashMap, HashSet};

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey, REGISTRY_LIST_PUBKEY_B64};
use schema_types::{FoldDbError, FoldDbResult, Schema};

use super::records::{relist, upsert_definitions};
use super::{CatalogFeedReport, CatalogFeedSource, CATALOG_FEED_IMAGE_VERSION};
use crate::field_name_policy::{schema_field_names, NAME_POLICY_VERSION};
use crate::types::CanonicalField;
use crate::vocabulary_commit::is_vocabulary_conflict;
use crate::{CanonicalVocabularyWrite, SchemaServiceState};

const MAX_VOCABULARY_RETRIES: usize = 10;

/// The inputs and the report of one run.
pub(super) struct FeedRun<'a> {
    pub(super) source: &'a dyn CatalogFeedSource,
    pub(super) trust: VerifyingKey,
    pub(super) now: String,
    pub(super) report: CatalogFeedReport,
}

/// New-field candidates from the touched definitions. The first definition
/// of a name wins; a later different one is left to admission next time.
fn vocabulary_candidates(touched: &[Schema]) -> Vec<(String, CanonicalField)> {
    let mut names = HashSet::new();
    let mut candidates = Vec::new();
    for schema in touched {
        for name in schema_field_names(schema) {
            let (Some(description), Some(field_type)) = (
                schema.field_descriptions.get(name),
                schema.field_types.get(name),
            ) else {
                continue;
            };
            if !names.insert(name.clone()) {
                continue;
            }
            candidates.push((
                name.clone(),
                CanonicalField {
                    description: description.clone(),
                    field_type: field_type.clone(),
                    version: schema.field_versions.get(name).copied().unwrap_or(1),
                    name_policy_version: NAME_POLICY_VERSION,
                    classification: schema.field_data_classifications.get(name).cloned(),
                    interest_category: schema.field_interest_categories.get(name).cloned(),
                },
            ));
        }
    }
    candidates
}

impl SchemaServiceState {
    /// Run the catalog feed once against `source`.
    ///
    /// # Errors
    /// The pinned registry key does not parse, a storage read or write
    /// fails, or another run wrote the image first.
    pub async fn run_catalog_feed(
        &self,
        source: &dyn CatalogFeedSource,
    ) -> FoldDbResult<CatalogFeedReport> {
        let trust = verifying_key_from_base64(REGISTRY_LIST_PUBKEY_B64)
            .map_err(|error| FoldDbError::Config(format!("registry list key: {error}")))?;
        let backend = self.storage.backend();
        let stored = backend.load_catalog_feed().await?;
        let unsupported = stored
            .as_ref()
            .map(|image| image.version)
            .filter(|version| *version != CATALOG_FEED_IMAGE_VERSION);
        if let Some(version) = unsupported {
            return Err(FoldDbError::Config(format!(
                "catalog feed image version {version} is not supported (reads {CATALOG_FEED_IMAGE_VERSION})"
            )));
        }
        let expected_revision = stored.as_ref().map(|image| image.revision);
        let mut image = stored.unwrap_or_default();
        let mut run = FeedRun {
            source,
            trust,
            now: self.clock.rfc3339_secs(),
            report: CatalogFeedReport::default(),
        };
        run.refresh_lists(&mut image.state).await;
        let definitions = run.read_manifests(&mut image.state).await;
        // Records change only through a changed list or a manifest read, so
        // the image changed exactly when one of the two happened.
        let changed = !run.report.lists_changed.is_empty()
            || !run.report.manifests_read.is_empty()
            || !run.report.manifest_errors.is_empty();
        let touched = upsert_definitions(&mut image, definitions, &run.now, &mut run.report);
        relist(&mut image, &run.now, &mut run.report);
        self.admit_feed_vocabulary(&touched, &mut run.report)
            .await?;
        if changed {
            image.revision = expected_revision.unwrap_or(0) + 1;
            backend.save_catalog_feed(&image, expected_revision).await?;
            run.report.image_written = true;
        }
        Ok(run.report)
    }

    /// Offer the new field names of the touched definitions to the canonical
    /// vocabulary, one field at a time, through the admission policy the
    /// seeds use. A refused field is logged and skipped; it never blocks a
    /// catalog record. Embeddings for admitted fields are computed as
    /// `warm_canonical_field_embeddings` computes them.
    async fn admit_feed_vocabulary(
        &self,
        touched: &[Schema],
        report: &mut CatalogFeedReport,
    ) -> FoldDbResult<()> {
        let candidates = vocabulary_candidates(touched);
        if candidates.is_empty() {
            return Ok(());
        }
        let backend = self.storage.backend();
        for _ in 0..MAX_VOCABULARY_RETRIES {
            let expected = backend.load_all_canonical_fields().await?;
            let (fields, skipped) = self.admit_new_fields(&expected, &candidates);
            report.canonical_fields_skipped = skipped;
            if fields.is_empty() {
                return Ok(());
            }
            let vocabulary = CanonicalVocabularyWrite { expected, fields };
            match backend
                .save_schema_definitions_if_vocabulary(&[], &vocabulary)
                .await
            {
                Ok(()) => {
                    self.cache_committed_vocabulary(&vocabulary.fields)?;
                    report.canonical_fields_added = vocabulary
                        .fields
                        .into_iter()
                        .map(|(name, _)| name)
                        .collect();
                    report.embeddings_computed = self.warm_canonical_field_embeddings().await.0;
                    return Ok(());
                }
                Err(error) if is_vocabulary_conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Err(FoldDbError::Config(format!(
            "canonical_vocabulary_write_conflict: the catalog feed exceeded {MAX_VOCABULARY_RETRIES} retries"
        )))
    }

    /// The candidates admission accepts against `expected`, and the count
    /// it refused. A name already in the vocabulary is not a candidate.
    fn admit_new_fields(
        &self,
        expected: &HashMap<String, CanonicalField>,
        candidates: &[(String, CanonicalField)],
    ) -> (Vec<(String, CanonicalField)>, usize) {
        let mut accepted = expected.clone();
        let mut admitted = Vec::new();
        let mut skipped = 0;
        for (name, field) in candidates {
            if accepted.contains_key(name) {
                continue;
            }
            match self.checked_vocabulary_batch_against(&accepted, &[(name.clone(), field.clone())])
            {
                Ok(checked) => {
                    for (name, field) in checked {
                        accepted.insert(name.clone(), field.clone());
                        admitted.push((name, field));
                    }
                }
                Err(error) => {
                    skipped += 1;
                    tracing::info!(
                        target: "schema_service::catalog_feed",
                        field = %name,
                        reason = %error,
                        "field not admitted to the canonical vocabulary"
                    );
                }
            }
        }
        (admitted, skipped)
    }
}
