//! Pure wire/domain schema types for schema_service.
//!
//! No storage, HTTP, fold_db, atom, or FieldVariant. Lambda and other
//! fold_db-free consumers depend on this crate instead of `fold_db`.

pub mod clock;
pub mod data_classification;
pub mod declarative_schemas;
pub mod error;
pub mod field_identity;
pub mod field_value_type;
pub mod hex;
pub mod key_config;
pub mod schema_type;

pub use data_classification::{
    DataClassification, CONFIDENTIAL, HIGHLY_RESTRICTED, INTERNAL, MAX_SENSITIVITY_LEVEL, PUBLIC,
    RESTRICTED,
};
pub use declarative_schemas::{
    canonical_name_parts, compute_identity_hash_parts, derive_transform_metadata, hash_expression,
    parse_canonical_name, refuses_identity_downgrade, DeclarativeSchemaDefinition, FieldMapper,
    IdentityRecompute, RecordMapper, SchemaSource, TransformMetadata, IDENTITY_HASH_ALGO_VERSION,
    LEGACY_IDENTITY_HASH_ALGO_VERSION, RECORD_SENTINEL,
};
pub use error::{FoldDbError, FoldDbResult};
pub use field_identity::{
    compute_declared_field_identity, compute_field_hash, FieldIdentityInputs,
    DECLARED_FIELD_IDENTITY_ALGO_VERSION,
};
pub use field_value_type::FieldValueType;
pub use key_config::KeyConfig;
pub use schema_type::DeclarativeSchemaType;

/// Type alias used throughout schema_service.
pub type Schema = DeclarativeSchemaDefinition;
/// Historical alias used by some call sites.
pub type SchemaType = DeclarativeSchemaType;
