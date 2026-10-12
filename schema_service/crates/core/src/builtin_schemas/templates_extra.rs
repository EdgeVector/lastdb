use super::builder::SchemaBuilder;
use super::templates_core::{nullable_string, string_array};
use super::{DOCUMENT_RECORD, GRAPH_EDGE, GRAPH_NODE, MEDIA_ASSET, SECURE_KV};
use schema_types::{FieldValueType, Schema, HIGHLY_RESTRICTED, PUBLIC};

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
