//! Schema constructors that used to ship as boot seeds.
//!
//! Cold start does not install TriggerFiring or the starter templates.
//! Canonical field seed is separate and still runs. The constructors stay
//! so a caller can build one of these schemas. Tom, 2026-10-14.
//!
//! `seed(&state)` still walks `all_phase_1_schemas()`. That list is empty,
//! so boot adds nothing. An identity that is already stored stays stored.

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

/// Schemas installed at boot. Empty: cold start adds no schema.
pub fn all_phase_1_schemas() -> Vec<Schema> {
    Vec::new()
}

/// Descriptive names of schemas installed at boot. Empty.
pub const PHASE_1_DESCRIPTIVE_NAMES: &[&str] = &[];
