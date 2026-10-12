use super::builder::SchemaBuilder;
use super::{EVENT_RECORD, KNOWLEDGE_RECORD, STATE_SNAPSHOT, WORK_ITEM};
use schema_types::{FieldValueType, Schema, PUBLIC};

pub(super) fn nullable_string() -> FieldValueType {
    FieldValueType::OneOf(vec![FieldValueType::String, FieldValueType::Null])
}

pub(super) fn string_array() -> FieldValueType {
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
