use serde_json::Value;

use super::*;

/// Offline system-schema heuristic for snapshot projection when the live
/// `system_schema_hashes` set is not available (e.g. pack publisher reading
/// a JSON file). Matches seed sources that classify as system-owned.
pub fn offline_is_system_schema(schema: &Schema) -> bool {
    matches!(
        schema.source,
        schema_types::SchemaSource::SystemSeed | schema_types::SchemaSource::StarterSeed
    )
}

/// Project a full registry snapshot to the shared-only resolver dataset.
///
/// Keeps [`crate::shared_surface::RegistrationClass::Shared`] and
/// [`crate::shared_surface::RegistrationClass::SystemOwned`] active rows,
/// drops private legacy bootstrap / unknown / superseded schemas, and
/// trims descriptive-name and field embedding caches to surviving rows.
/// Canonical-field registry + embeddings stay (they are shared vocabulary).
///
/// `is_system` classifies system seeds the same way as
/// [`SchemaServiceState::is_system_schema`] when available; pack publishers
/// should pass [`offline_is_system_schema`].
pub fn project_snapshot_shared_only<F>(
    mut envelope: SnapshotEnvelope,
    mut is_system: F,
) -> SnapshotEnvelope
where
    F: FnMut(&Schema) -> bool,
{
    use crate::shared_surface::{include_in_shared_only_projection, SharedSurfaceAttachment};
    use std::collections::HashSet;

    let attachment = SharedSurfaceAttachment::default();
    envelope
        .schemas
        .retain(|schema| include_in_shared_only_projection(schema, is_system(schema), &attachment));
    envelope.schemas.sort_by(|a, b| a.name.cmp(&b.name));

    let mut kept_desc_names: HashSet<String> = HashSet::new();
    let mut kept_schema_names: HashSet<String> = HashSet::new();
    for schema in &envelope.schemas {
        kept_schema_names.insert(schema.name.clone());
        if let Some(d) = schema.descriptive_name.as_ref() {
            kept_desc_names.insert(d.clone());
        }
        kept_desc_names.insert(schema.name.clone());
        // Namespaced descriptive-name keys used by the pack publisher.
        if let Some(owner) = schema.owner_app_id.as_deref() {
            if let Some(d) = schema.descriptive_name.as_ref() {
                kept_desc_names.insert(format!("{owner}/{d}"));
            }
        }
    }

    envelope
        .embeddings
        .descriptive_names
        .retain(|k, _| kept_desc_names.contains(k) || kept_schema_names.contains(k));

    // Field embedding keys are `{descriptive_name}:{field_name}:{desc_hash}`
    // (see pack publisher `field_embedding_cache_key`). Keep any key whose
    // first segment matches a surviving schema descriptive name.
    envelope.embeddings.fields.retain(|k, _| {
        let head = k.split(':').next().unwrap_or(k);
        kept_desc_names.contains(head) || kept_schema_names.contains(head)
    });

    envelope
}

/// The snapshot export both HTTP routers answer.
///
/// Success is HTTP 200 and the envelope with embeddings removed. Failure is
/// HTTP 500. `message` is the error text. The Lambda router still checks
/// `X-API-Key`. The actix router still writes the log. `body` is
/// `{ "error": message }`.
#[derive(Debug)]
pub enum SnapshotExportReply {
    /// The export finished. The envelope has embeddings removed.
    Ready(Box<SnapshotEnvelope>),
    /// The export failed. HTTP 500.
    ///
    /// `message` is `{prefix}: {error}`. The actix router still writes the
    /// error log. `body` is `{ "error": message }`.
    Failed { message: String, body: Value },
}

impl SnapshotExportReply {
    /// The snapshot envelope, or the HTTP error.
    ///
    /// Ready stays typed so each router serializes it. Failed is 500.
    /// The log line is the actix error text. The Lambda router ignores
    /// that line. The Lambda router still checks `X-API-Key`.
    pub fn into_http(self) -> Result<Box<SnapshotEnvelope>, (u16, Value, String)> {
        match self {
            Self::Ready(envelope) => Ok(envelope),
            Self::Failed { message, body } => Err((500, body, message)),
        }
    }
}

fn classify_snapshot_export(
    exported: FoldDbResult<SnapshotEnvelope>,
    failure_prefix: &str,
) -> SnapshotExportReply {
    match exported {
        Ok(envelope) => SnapshotExportReply::Ready(Box::new(envelope.without_embeddings())),
        Err(error) => {
            let message = format!("{failure_prefix}: {error}");
            let body = crate::error_only_body(&message);
            SnapshotExportReply::Failed { message, body }
        }
    }
}

impl SchemaServiceState {
    /// Capture the current registry state as a [`SnapshotEnvelope`].
    ///
    /// Pulls read locks across every cache the registry maintains. The
    /// envelope is fully self-contained; no follow-up calls are required
    /// for an importer to reach a working state.
    ///
    /// **Full registry** (includes private legacy bootstrap rows). For
    /// resolver-pack publishing use [`Self::export_shared_only_snapshot`].
    pub fn export_snapshot(&self) -> FoldDbResult<SnapshotEnvelope> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        let canonical_fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        let descriptive_name_embeddings = read_lock(
            &self.descriptive_name_embeddings,
            "descriptive_name_embeddings",
        )?;
        let field_embeddings = read_lock(&self.field_embeddings, "field_embeddings")?;
        let canonical_field_embeddings = read_lock(
            &self.canonical_field_embeddings,
            "canonical_field_embeddings",
        )?;

        // Sort the Vec fields by their natural key so the envelope serializes
        // to identical bytes across reads of identical state. canonical_fields
        // is serialized through a BTreeMap (see `serialize_canonical_fields_sorted`).
        let mut schemas_vec: Vec<Schema> = schemas
            .values()
            .filter(|schema| !crate::builtin_schemas::is_schema_org_leftover(schema))
            .cloned()
            .collect();
        schemas_vec.sort_by(|a, b| a.name.cmp(&b.name));
        let apps = read_lock(&self.apps, "apps")?;
        let mut apps_vec: Vec<AppRecord> = apps.values().cloned().collect();
        apps_vec.sort_by(|a, b| a.app_id.cmp(&b.app_id));

        Ok(SnapshotEnvelope {
            format_version: SNAPSHOT_FORMAT_VERSION,
            version: self.current_state_version(),
            captured_at: self.clock.rfc3339_secs(),
            embedder_version: self.embedder.embedder_id().to_string(),
            schemas: schemas_vec,
            // Populated from the canonical app registry (app_identity v3.1
            // Lane B2b). Sorted by app_id for byte-stable snapshot output.
            apps: apps_vec,
            canonical_fields: canonical_fields.clone(),
            embeddings: SnapshotEmbeddings {
                descriptive_names: descriptive_name_embeddings.clone(),
                fields: field_embeddings.clone(),
                canonical_fields: canonical_field_embeddings.clone(),
            },
        })
    }

    /// Capture a **shared-only** snapshot for resolver-pack publishing.
    ///
    /// Equivalent to [`Self::export_snapshot`] followed by
    /// [`project_snapshot_shared_only`]. Private legacy bootstrap and
    /// unknown rows never appear in the result.
    pub fn export_shared_only_snapshot(&self) -> FoldDbResult<SnapshotEnvelope> {
        let full = self.export_snapshot()?;
        Ok(project_snapshot_shared_only(full, |schema| {
            let key = schema
                .identity_hash
                .as_deref()
                .unwrap_or(schema.name.as_str());
            self.is_system_schema(key)
                || self.is_system_schema(&schema.name)
                || offline_is_system_schema(schema)
        }))
    }

    /// Classify [`Self::export_snapshot`] for the HTTP routers.
    ///
    /// A failure stays HTTP 500. The body is `{ "error": message }`. The
    /// export method stays public. Each router still writes a success
    /// response. The actix router still writes the error log.
    #[must_use]
    pub fn export_snapshot_reply(&self) -> SnapshotExportReply {
        classify_snapshot_export(self.export_snapshot(), "Failed to export snapshot")
    }

    /// Classify [`Self::export_shared_only_snapshot`] for the HTTP routers.
    ///
    /// A failure stays HTTP 500. The body is `{ "error": message }`. The
    /// export method stays public. Each router still writes a success
    /// response. The actix router still writes the error log.
    #[must_use]
    pub fn export_shared_only_snapshot_reply(&self) -> SnapshotExportReply {
        classify_snapshot_export(
            self.export_shared_only_snapshot(),
            "Failed to export shared-only snapshot",
        )
    }
}
