//! SystemSeed schemas that every schema service ships with.
//!
//!
//! The service keeps a tiny set of system-owned schemas for platform
//! observability. Product feature schemas should be declared by apps or users,
//! not preloaded here.
//!
//! ## Seeding is idempotent
//!
//! `seed(&state)` iterates every built-in schema. For each, it
//! computes the identity_hash and checks whether the schema service
//! already has a schema by that hash. If present: no-op. If absent:
//! add. This makes seeding safe to re-run across restarts, against
//! fresh Sled stores, and against existing stores that may have
//! picked up the same schemas via the legacy propose flow.

use crate::state::SchemaServiceState;
use crate::types::SchemaAddOutcome;
use schema_types::DeclarativeSchemaType as SchemaType;
use schema_types::FieldValueType;
use schema_types::KeyConfig;
use schema_types::{DataClassification, HIGHLY_RESTRICTED, INTERNAL, PUBLIC};
use schema_types::{FoldDbError, FoldDbResult};
use schema_types::{Schema, SchemaSource};
use std::collections::HashMap;

pub const TRIGGER_FIRING: &str = "TriggerFiring";
pub const TEMPLATE_OWNER_APP_ID: &str = "templates";

pub const KNOWLEDGE_RECORD: &str = "KnowledgeRecord";
pub const EVENT_RECORD: &str = "EventRecord";
pub const STATE_SNAPSHOT: &str = "StateSnapshot";
pub const WORK_ITEM: &str = "WorkItem";
pub const DOCUMENT_RECORD: &str = "DocumentRecord";
pub const MEDIA_ASSET: &str = "MediaAsset";
pub const GRAPH_NODE: &str = "GraphNode";
pub const GRAPH_EDGE: &str = "GraphEdge";
pub const SECURE_KV: &str = "SecureKV";

/// Build a `Schema` value with the standard configuration:
/// - Hash schema_type by default (override where needed)
/// - descriptive_name = name (we keep them aligned for readability)
/// - field_types populated from the caller
/// - field_descriptions populated from the caller (required by schema service)
/// - sensitivity defaults to 0 unless specified per field
/// - identity_hash computed before return
struct SchemaBuilder {
    name: &'static str,
    schema_type: SchemaType,
    key: KeyConfig,
    source: SchemaSource,
    owner_app_id: Option<&'static str>,
    purpose_statement: Option<&'static str>,
    fields: Vec<(&'static str, FieldValueType, &'static str, u8)>, // (name, type, desc, sensitivity)
}

impl SchemaBuilder {
    fn hash_range(name: &'static str, hash_field: &'static str, range_field: &'static str) -> Self {
        Self {
            name,
            schema_type: SchemaType::HashRange,
            key: KeyConfig::new(Some(hash_field.to_string()), Some(range_field.to_string())),
            source: SchemaSource::SystemSeed,
            owner_app_id: None,
            purpose_statement: None,
            fields: Vec::new(),
        }
    }

    fn single_template(name: &'static str, purpose_statement: &'static str) -> Self {
        Self {
            name,
            schema_type: SchemaType::Single,
            key: KeyConfig::new(None, None),
            source: SchemaSource::StarterSeed,
            owner_app_id: Some(TEMPLATE_OWNER_APP_ID),
            purpose_statement: Some(purpose_statement),
            fields: Vec::new(),
        }
    }

    fn sensitive_field(
        mut self,
        name: &'static str,
        ty: FieldValueType,
        description: &'static str,
        sensitivity: u8,
    ) -> Self {
        self.fields.push((name, ty, description, sensitivity));
        self
    }

    fn build(self) -> Schema {
        let field_names: Vec<String> = self.fields.iter().map(|f| f.0.to_string()).collect();

        let mut schema = Schema::new(
            self.name.to_string(),
            self.schema_type,
            Some(self.key),
            Some(field_names),
            None,
            None,
        );

        schema.descriptive_name = Some(self.name.to_string());
        schema.source = self.source;
        schema.owner_app_id = self.owner_app_id.map(str::to_string);
        schema.purpose_statement = self.purpose_statement.map(str::to_string);

        for (name, ty, description, sensitivity) in self.fields {
            schema.field_types.insert(name.to_string(), ty);
            schema
                .field_descriptions
                .insert(name.to_string(), description.to_string());
            schema.field_data_classifications.insert(
                name.to_string(),
                DataClassification {
                    sensitivity_level: sensitivity,
                    data_domain: "general".to_string(),
                },
            );
            // Default classification so the schema service doesn't reject on
            // the classification-validation path used elsewhere in the codebase.
            let class = if matches!(schema.field_types.get(name), Some(FieldValueType::Integer)) {
                "number"
            } else if matches!(schema.field_types.get(name), Some(FieldValueType::Boolean)) {
                "boolean"
            } else {
                "word"
            };
            schema
                .field_classifications
                .insert(name.to_string(), vec![class.to_string()]);
        }

        schema.compute_identity_hash();
        schema
    }
}

// ────────────────────────────────────────────────────────────────────
//  Trigger observability
// ────────────────────────────────────────────────────────────────────

/// Internal log of every view trigger firing — one row per attempt,
/// win or lose. The trigger runner in fold_db writes here.
///
/// Field-name source of truth lives on the consumer side at
/// `fold_db/crates/core/src/triggers/mod.rs::fields` (`TRIGGER_ID`,
/// `VIEW_NAME`, …). The string literals below MUST match those
/// constants exactly — drift breaks the runner's writes silently.
/// The schema *shape* (types, key, classification) is owned here.
///
/// Every field is classified `INTERNAL` (sensitivity = 1): trigger
/// firings carry view names, error messages, and snapshot payloads
/// that are operator-visible but never user-displayed.
pub fn trigger_firing_schema() -> Schema {
    SchemaBuilder::hash_range(TRIGGER_FIRING, "trigger_id", "fired_at")
        .sensitive_field(
            "trigger_id",
            FieldValueType::String,
            "Stable id derived from the trigger config (e.g. `{view_id}:{index}`)",
            INTERNAL,
        )
        .sensitive_field(
            "view_name",
            FieldValueType::String,
            "Name of the view that fired",
            INTERNAL,
        )
        .sensitive_field(
            "fired_at",
            FieldValueType::Integer,
            "Milliseconds since Unix epoch when the firing began",
            INTERNAL,
        )
        .sensitive_field(
            "duration_ms",
            FieldValueType::Integer,
            "How long the firing took, in milliseconds",
            INTERNAL,
        )
        .sensitive_field(
            "status",
            FieldValueType::String,
            "Outcome: \"success\" | \"error\" | \"quarantined\" | \"skipped\"",
            INTERNAL,
        )
        .sensitive_field(
            "input_row_count",
            FieldValueType::Integer,
            "Rows read from source schemas",
            INTERNAL,
        )
        .sensitive_field(
            "output_row_count",
            FieldValueType::Integer,
            "Rows written to the output schema",
            INTERNAL,
        )
        .sensitive_field(
            "error_message",
            FieldValueType::OneOf(vec![FieldValueType::String, FieldValueType::Null]),
            "Error detail when status != \"success\"",
            INTERNAL,
        )
        .sensitive_field(
            "skip_reason",
            FieldValueType::OneOf(vec![FieldValueType::String, FieldValueType::Null]),
            "Reason when status == \"skipped\": \"skip_if_idle\" | \"dirty_clean\" | \"catch_up_budget\"; Null otherwise",
            INTERNAL,
        )
        .sensitive_field(
            "input_snapshot",
            // `Any` is the closest fit for an opaque JSON envelope —
            // there is no `Json` variant in `FieldValueType`. `Any`
            // already accepts `Null`, so we don't wrap it in `OneOf`.
            FieldValueType::Any,
            "Captured input envelope (JSON) or Null when no snapshot was taken (TH6a)",
            INTERNAL,
        )
        .sensitive_field(
            "schema_versions",
            FieldValueType::Any,
            "Map of input schema name to 64-char hex identity hash, captured at firing time (TH6a)",
            INTERNAL,
        )
        .sensitive_field(
            "snapshot_truncated",
            FieldValueType::Boolean,
            "True when the input envelope was truncated to fit the 10 MiB cap (TH6a)",
            INTERNAL,
        )
        .build()
}

// --------------------------------------------------------------------
//  App starter schema templates
// --------------------------------------------------------------------

fn nullable_string() -> FieldValueType {
    FieldValueType::OneOf(vec![FieldValueType::String, FieldValueType::Null])
}

fn string_array() -> FieldValueType {
    FieldValueType::Array(Box::new(FieldValueType::String))
}

pub fn knowledge_record_schema() -> Schema {
    SchemaBuilder::single_template(
        KNOWLEDGE_RECORD,
        "A durable user-authored knowledge item whose app-level category lives in the kind field.",
    )
    .sensitive_field(
        "slug",
        FieldValueType::String,
        "Stable record slug.",
        PUBLIC,
    )
    .sensitive_field(
        "title",
        FieldValueType::String,
        "Human-readable title.",
        PUBLIC,
    )
    .sensitive_field(
        "body",
        FieldValueType::String,
        "Main note or document body.",
        PUBLIC,
    )
    .sensitive_field(
        "kind",
        FieldValueType::String,
        "App-level category discriminator.",
        PUBLIC,
    )
    .sensitive_field("tags", string_array(), "User or app tags.", PUBLIC)
    .sensitive_field(
        "status",
        nullable_string(),
        "Workflow status, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "created_at",
        FieldValueType::String,
        "Creation timestamp.",
        PUBLIC,
    )
    .sensitive_field(
        "updated_at",
        FieldValueType::String,
        "Last update timestamp.",
        PUBLIC,
    )
    .build()
}

pub fn event_record_schema() -> Schema {
    SchemaBuilder::single_template(
        EVENT_RECORD,
        "An append-only event describing who did what to a subject at a point in time.",
    )
    .sensitive_field(
        "event_id",
        FieldValueType::String,
        "Stable event id.",
        PUBLIC,
    )
    .sensitive_field("at", FieldValueType::String, "Event timestamp.", PUBLIC)
    .sensitive_field(
        "actor",
        FieldValueType::String,
        "Actor that caused the event.",
        PUBLIC,
    )
    .sensitive_field(
        "kind",
        FieldValueType::String,
        "Event category discriminator.",
        PUBLIC,
    )
    .sensitive_field(
        "subject",
        FieldValueType::String,
        "Thing the event is about.",
        PUBLIC,
    )
    .sensitive_field(
        "from_state",
        nullable_string(),
        "Previous state, or null.",
        PUBLIC,
    )
    .sensitive_field("to_state", nullable_string(), "New state, or null.", PUBLIC)
    .sensitive_field(
        "detail",
        FieldValueType::Any,
        "Event-specific detail payload.",
        PUBLIC,
    )
    .sensitive_field(
        "schema_version",
        FieldValueType::Integer,
        "Event payload schema version.",
        PUBLIC,
    )
    .build()
}

pub fn state_snapshot_schema() -> Schema {
    SchemaBuilder::single_template(
        STATE_SNAPSHOT,
        "A replace-in-place snapshot of the current state of one subject.",
    )
    .sensitive_field(
        "subject",
        FieldValueType::String,
        "Thing whose state is recorded.",
        PUBLIC,
    )
    .sensitive_field(
        "state",
        FieldValueType::String,
        "Current state label.",
        PUBLIC,
    )
    .sensitive_field(
        "detail",
        FieldValueType::Any,
        "State-specific detail payload.",
        PUBLIC,
    )
    .sensitive_field(
        "schema_version",
        FieldValueType::Integer,
        "State payload schema version.",
        PUBLIC,
    )
    .sensitive_field(
        "updated_at",
        FieldValueType::String,
        "Last update timestamp.",
        PUBLIC,
    )
    .build()
}

pub fn work_item_schema() -> Schema {
    SchemaBuilder::single_template(
        WORK_ITEM,
        "A trackable unit of work with dependencies, ownership, priority, and timestamps.",
    )
    .sensitive_field(
        "slug",
        FieldValueType::String,
        "Stable work item slug.",
        PUBLIC,
    )
    .sensitive_field(
        "title",
        FieldValueType::String,
        "Human-readable title.",
        PUBLIC,
    )
    .sensitive_field("body", FieldValueType::String, "Work item details.", PUBLIC)
    .sensitive_field("status", FieldValueType::String, "Workflow status.", PUBLIC)
    .sensitive_field(
        "parent",
        nullable_string(),
        "Optional parent work item.",
        PUBLIC,
    )
    .sensitive_field(
        "deps",
        string_array(),
        "Dependency work item slugs.",
        PUBLIC,
    )
    .sensitive_field("tags", string_array(), "Tags or labels.", PUBLIC)
    .sensitive_field(
        "assignee",
        nullable_string(),
        "Assigned actor, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "priority",
        nullable_string(),
        "Priority label, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "created_at",
        FieldValueType::String,
        "Creation timestamp.",
        PUBLIC,
    )
    .sensitive_field(
        "updated_at",
        FieldValueType::String,
        "Last update timestamp.",
        PUBLIC,
    )
    .build()
}

pub fn document_record_schema() -> Schema {
    SchemaBuilder::single_template(
        DOCUMENT_RECORD,
        "A document or extracted document payload with source metadata and typed content.",
    )
    .sensitive_field("title", FieldValueType::String, "Document title.", PUBLIC)
    .sensitive_field(
        "source_file",
        nullable_string(),
        "Original file or source URI.",
        PUBLIC,
    )
    .sensitive_field(
        "file_type",
        FieldValueType::String,
        "Document file or media type.",
        PUBLIC,
    )
    .sensitive_field(
        "content",
        FieldValueType::String,
        "Extracted or authored text content.",
        PUBLIC,
    )
    .sensitive_field(
        "doc_kind",
        FieldValueType::String,
        "Document kind discriminator.",
        PUBLIC,
    )
    .sensitive_field(
        "extracted_at",
        nullable_string(),
        "Extraction timestamp, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "metadata",
        FieldValueType::Any,
        "Document-specific metadata.",
        PUBLIC,
    )
    .build()
}

pub fn media_asset_schema() -> Schema {
    SchemaBuilder::single_template(
        MEDIA_ASSET,
        "A media asset such as an image or screenshot with dimensions and display metadata.",
    )
    .sensitive_field(
        "asset_id",
        FieldValueType::String,
        "Stable media asset id.",
        PUBLIC,
    )
    .sensitive_field(
        "uri",
        FieldValueType::String,
        "Asset URI or blob reference.",
        PUBLIC,
    )
    .sensitive_field("mime", FieldValueType::String, "Media MIME type.", PUBLIC)
    .sensitive_field(
        "width",
        nullable_string(),
        "Pixel width, or null when unknown.",
        PUBLIC,
    )
    .sensitive_field(
        "height",
        nullable_string(),
        "Pixel height, or null when unknown.",
        PUBLIC,
    )
    .sensitive_field(
        "caption",
        nullable_string(),
        "Display caption, or null.",
        PUBLIC,
    )
    .sensitive_field("tags", string_array(), "Asset tags.", PUBLIC)
    .sensitive_field(
        "created_at",
        FieldValueType::String,
        "Creation timestamp.",
        PUBLIC,
    )
    .build()
}

pub fn graph_node_schema() -> Schema {
    SchemaBuilder::single_template(
        GRAPH_NODE,
        "A graph node with typed properties for relationships and knowledge graphs.",
    )
    .sensitive_field("id", FieldValueType::String, "Stable node id.", PUBLIC)
    .sensitive_field(
        "kind",
        FieldValueType::String,
        "Node kind discriminator.",
        PUBLIC,
    )
    .sensitive_field("label", FieldValueType::String, "Display label.", PUBLIC)
    .sensitive_field(
        "props",
        FieldValueType::Any,
        "Node-specific properties.",
        PUBLIC,
    )
    .build()
}

pub fn graph_edge_schema() -> Schema {
    SchemaBuilder::single_template(
        GRAPH_EDGE,
        "A graph edge connecting two graph nodes with a relation kind and optional weight.",
    )
    .sensitive_field("from_id", FieldValueType::String, "Source node id.", PUBLIC)
    .sensitive_field("to_id", FieldValueType::String, "Target node id.", PUBLIC)
    .sensitive_field("kind", FieldValueType::String, "Relationship kind.", PUBLIC)
    .sensitive_field(
        "weight",
        FieldValueType::Number,
        "Optional relationship strength.",
        PUBLIC,
    )
    .build()
}

pub fn secure_kv_schema() -> Schema {
    SchemaBuilder::single_template(
        SECURE_KV,
        "A secret or credential reference whose secret_value requires restricted handling.",
    )
    .sensitive_field(
        "slug",
        FieldValueType::String,
        "Stable secret slug.",
        PUBLIC,
    )
    .sensitive_field(
        "label",
        FieldValueType::String,
        "Human-readable label.",
        PUBLIC,
    )
    .sensitive_field(
        "secret_value",
        FieldValueType::String,
        "Secret value or secure secret locator.",
        HIGHLY_RESTRICTED,
    )
    .sensitive_field(
        "provider",
        nullable_string(),
        "Secret provider, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "environment",
        nullable_string(),
        "Environment label, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "purpose",
        nullable_string(),
        "Operational purpose, or null.",
        PUBLIC,
    )
    .sensitive_field(
        "created_at",
        FieldValueType::String,
        "Creation timestamp.",
        PUBLIC,
    )
    .sensitive_field(
        "updated_at",
        FieldValueType::String,
        "Last update timestamp.",
        PUBLIC,
    )
    .build()
}

pub fn app_starter_template_schemas() -> Vec<Schema> {
    vec![
        knowledge_record_schema(),
        event_record_schema(),
        state_snapshot_schema(),
        work_item_schema(),
        document_record_schema(),
        media_asset_schema(),
        graph_node_schema(),
        graph_edge_schema(),
        secure_kv_schema(),
    ]
}

pub fn is_app_starter_template(schema: &Schema) -> bool {
    schema.source == SchemaSource::StarterSeed
        && schema.owner_app_id.as_deref() == Some(TEMPLATE_OWNER_APP_ID)
}

/// Schema.org type dump leftover: `starter_seed` that is not an app template.
/// These must not appear in default available / resolve (Tom 2026-08-17).
pub fn is_schema_org_leftover(schema: &Schema) -> bool {
    schema.source == SchemaSource::StarterSeed && !is_app_starter_template(schema)
}

// ────────────────────────────────────────────────────────────────────
//  Registration order
// ────────────────────────────────────────────────────────────────────

/// Return every service-owned seed schema that should be installed at boot.
pub fn all_phase_1_schemas() -> Vec<Schema> {
    let mut schemas = vec![trigger_firing_schema()];
    schemas.extend(app_starter_template_schemas());
    schemas
}

/// The list of all descriptive names for the built-in schemas. Clients
/// use this list to identify service-owned schemas.
pub const PHASE_1_DESCRIPTIVE_NAMES: &[&str] = &[TRIGGER_FIRING];

/// Seed every built-in schema into the given `SchemaServiceState`.
///
/// Idempotent: schemas whose identity_hash is already present in the
/// service are skipped. Schemas that are missing get added via
/// `state.add_schema()`, which is the same path user-data schemas
/// take. The only semantic difference is the origin — these come
/// from the service's own Rust code, not from a client proposal.
///
/// Called from `SchemaServiceServer::new()` so every production
/// schema service instance auto-seeds on boot. Tests that spin up a
/// `SchemaServiceState` directly must call this explicitly before
/// handing the state to an HTTP layer.
///
/// Fails loudly on the first error. Partial seeding is not a valid
/// state for the service — if we can't install the built-ins, the
/// service should refuse to start.
pub async fn seed(state: &SchemaServiceState) -> FoldDbResult<()> {
    // Built-ins are authoritative — they must register exactly, never fuzzy
    // reuse-before-NEW into a semantically-near committed seed (which would
    // trip the "built-ins never expand" invariant below). Gate the fuzzy
    // path off for the duration of this seed load.
    state.set_seeding_in_progress(true);
    let result = seed_inner(state).await;
    state.set_seeding_in_progress(false);
    result
}

async fn seed_inner(state: &SchemaServiceState) -> FoldDbResult<()> {
    for schema in all_phase_1_schemas() {
        let descriptive = schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone());
        match state.add_schema(schema, HashMap::new()).await {
            Ok(
                SchemaAddOutcome::Added(added, _)
                | SchemaAddOutcome::AlreadyExists(added, _)
                | SchemaAddOutcome::Composed(added, _, _),
            ) => {
                // Both outcomes are fine. AlreadyExists fires when the
                // service was previously seeded (or a client
                // previously proposed this schema via the legacy
                // propose flow). Idempotent either way.
                //
                // Tag the resulting identity hash as a system schema
                // so downstream clients (fold_db_node UI) can group
                // infrastructure schemas separately from user data.
                if added.source == SchemaSource::SystemSeed {
                    state.mark_system_schema(added.name.clone());
                }
            }
            Ok(SchemaAddOutcome::Expanded(old, _, _)) => {
                return Err(FoldDbError::Config(format!(
                    "builtin_schemas: '{descriptive}' unexpectedly expanded existing schema '{old}'. \
                     Built-in schemas must not collide with user-proposed schemas."
                )));
            }
            Ok(SchemaAddOutcome::DescriptiveNameConflict(conflict)) => {
                return Err(FoldDbError::Config(format!(
                    "builtin_schemas: '{}' conflicted with existing schema '{}': {}",
                    descriptive, conflict.existing_canonical, conflict.reason
                )));
            }
            Err(e) => {
                return Err(FoldDbError::Config(format!(
                    "builtin_schemas: failed to seed '{descriptive}': {e}"
                )));
            }
        }
    }
    Ok(())
}
