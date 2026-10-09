use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// An immutable data container that represents a single version of content in the database.
///
/// Atoms are the fundamental building blocks of the database's immutable data storage system.
/// Each Atom contains:
/// - A unique identifier (content-addressed)
/// - The source schema that defines its structure
/// - The public key of the creator
/// - Creation timestamp
/// - The actual content data
///
/// Version history is tracked via the delta event log (`MutationEvent`),
/// not via atom-level chaining. Once created, an Atom's content cannot be modified.
///
/// # Content size limit (hard fence)
///
/// Field payloads written through the mutation / atom-store path must fit under
/// [`crate::atom::max_atom_content_bytes`] (**default 64 KiB**; env
/// `LASTDB_MAX_ATOM_CONTENT_BYTES`, absolute max 1 MiB). Oversized content is
/// rejected with [`crate::schema::types::SchemaError::AtomContentTooLarge`].
/// Atoms are **not** a blob store — large/opaque bytes go in file-blob / CAS.
/// See `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Atom {
    uuid: String,
    source_schema_name: String,
    source_file_name: Option<String>,
    /// General-purpose metadata (e.g., file_hash, source info).
    /// Not included in content-based UUID — metadata doesn't affect deduplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
    created_at: DateTime<Utc>,
    content: Value,
}

impl Atom {
    /// Generates a deterministic UUID based on atom content.
    /// This enables content-based deduplication at the atom level.
    ///
    /// # Arguments
    ///
    /// * `source_schema_name` - Name of the schema that defines this Atom's structure
    /// * `content` - The actual data content stored in this Atom
    ///
    /// # Returns
    ///
    /// A deterministic UUID string based on SHA256 hash of schema name and content.
    /// Each variable-length field is preceded by a 4-byte big-endian length prefix
    /// so that distinct `(schema, content)` pairs cannot share the same hash input
    /// by shifting bytes across the schema/content boundary. Same bug class and
    /// fix as PR #408 (share-rule canonical bytes), PR #409 (`MoleculeHashRange`)
    /// and PR #422 (`hash_input_snapshot`). Pinned by
    /// `schema_suffix_does_not_collide_with_content_prefix`.
    fn generate_content_uuid(source_schema_name: &str, content: &Value) -> String {
        let mut hasher = Sha256::new();
        let schema_bytes = source_schema_name.as_bytes();
        let content_string = content.to_string();
        let content_bytes = content_string.as_bytes();
        hasher.update((schema_bytes.len() as u32).to_be_bytes());
        hasher.update(schema_bytes);
        hasher.update((content_bytes.len() as u32).to_be_bytes());
        hasher.update(content_bytes);
        let hash = hasher.finalize();
        format!("{hash:x}")
    }

    /// Creates a new Atom with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `source_schema_name` - Name of the schema that defines this Atom's structure
    /// * `content` - The actual data content stored in this Atom
    ///
    /// # Returns
    ///
    /// A new Atom instance with a content-based UUID and current timestamp
    #[must_use]
    pub fn new(source_schema_name: String, content: Value) -> Self {
        let uuid = Self::generate_content_uuid(&source_schema_name, &content);
        Self {
            uuid,
            source_schema_name,
            source_file_name: None,
            metadata: None,
            created_at: Utc::now(),
            content,
        }
    }

    /// Restore trusted in-memory parts without serialization, rehashing, or
    /// changing the original creation time.
    pub(crate) fn from_stored_parts(
        uuid: String,
        source_schema_name: String,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
        created_at: DateTime<Utc>,
        content: Value,
    ) -> Self {
        Self {
            uuid,
            source_schema_name,
            source_file_name,
            metadata,
            created_at,
            content,
        }
    }

    /// Sets the source file name for atoms created from file uploads
    #[must_use]
    pub fn with_source_file_name(mut self, file_name: String) -> Self {
        self.source_file_name = Some(file_name);
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    #[must_use]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.metadata.as_ref()
    }

    /// Returns a reference to the Atom's content.
    ///
    /// This method provides read-only access to the stored data,
    /// maintaining the immutability principle.
    #[must_use]
    pub const fn content(&self) -> &Value {
        &self.content
    }

    /// Applies a transformation to the Atom's content and returns the result.
    ///
    /// Currently supports:
    /// - "lowercase": Converts string content to lowercase
    ///
    /// Returns the unique identifier of this Atom.
    ///
    /// This UUID uniquely identifies this specific version of the data
    /// and is used by Molecules to point to the current version.
    #[must_use]
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    /// Returns the name of the schema that defines this Atom's structure.
    ///
    /// The schema name is used to validate the content structure and
    /// determine applicable permissions and payment requirements.
    #[must_use]
    pub fn source_schema_name(&self) -> &str {
        &self.source_schema_name
    }

    /// Returns the original filename if this atom was created from a file upload.
    ///
    /// This is used for tracking data provenance and auditing purposes.
    #[must_use]
    pub fn source_file_name(&self) -> Option<&String> {
        self.source_file_name.as_ref()
    }

    /// Returns the timestamp when this Atom was created.
    ///
    /// This timestamp is used for auditing and version history tracking.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns `true` if this atom's content is the reserved tombstone
    /// shape. See [`crate::atom::tombstone`] for the full predicate.
    /// Filtered out at molecule resolution unless the caller asks for
    /// `include_tombstones = true`.
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        super::tombstone::is_tombstone_value(&self.content)
    }
}
