use super::builder::SchemaBuilder;
use super::TRIGGER_FIRING;
use schema_types::{FieldValueType, Schema, INTERNAL};

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
