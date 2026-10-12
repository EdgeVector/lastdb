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

mod builder;
mod seed;
mod templates_core;
mod templates_extra;
mod trigger;

pub use seed::seed;
pub use templates_core::{
    event_record_schema, knowledge_record_schema, state_snapshot_schema, work_item_schema,
};
pub use templates_extra::{
    document_record_schema, graph_edge_schema, graph_node_schema, media_asset_schema,
    secure_kv_schema,
};
pub use trigger::trigger_firing_schema;

use schema_types::Schema;
use schema_types::SchemaSource;

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
