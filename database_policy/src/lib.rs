//! Spec catalog and share admission for named LastDB databases.
//!
//! The crate reads no clock and opens no file. [`SpecBinding::admitted`] and
//! [`admit_namespace`] are the narrow policy. A missing binding is
//! [`PolicyError::BindingAbsent`]. Callers deny that result.
//!
//! The registry, the publication marker, and the daemon share one binding.
//! [`SpecBinding::admitted`] is that binding. [`record_is_shareable`] is the
//! LastDB check for a schema, its fields, and a file. A service that stores
//! the log does not call it. The payload stays opaque there.
//!
//! Social schemas live in `spec/social/`. A database name does not select a
//! spec. The stored `{ "name", "version" }` binding does. `social-graph` stays
//! in `personal`.

/// Union of provisioned share namespaces, for callers that do not yet
/// carry a per-database binding. Named databases use [`admit_namespace`].
pub const SHAREABLE_NAMESPACES: &[&str] = &["lastdb-files", "social"];

const WHOLE_DATABASE: &str = "whole-database";

struct SchemaSpec {
    namespace: &'static str,
    name: &'static str,
    fields: &'static [&'static str],
}

struct ShareSpec {
    namespaces: &'static [&'static str],
    schemas: &'static [SchemaSpec],
}

const SOCIAL: ShareSpec = ShareSpec {
    namespaces: &["social"],
    schemas: &[
        SchemaSpec {
            namespace: "social",
            name: "Card",
            fields: &["person_id", "display_name", "avatar"],
        },
        SchemaSpec {
            namespace: "social",
            name: "Photo",
            fields: &[
                "photo_taken_at",
                "photo_caption",
                "image_width_pixels",
                "image_height_pixels",
                "file_media_type",
                "file_size_bytes",
                "camera_make",
                "camera_model",
                "geographic_latitude_degrees",
                "geographic_longitude_degrees",
            ],
        },
        SchemaSpec {
            namespace: "social",
            name: "Album",
            fields: &["title", "description", "cover_photo_id", "created_at"],
        },
        SchemaSpec {
            namespace: "social",
            name: "AlbumItem",
            fields: &["photo_id", "added_by", "position"],
        },
        SchemaSpec {
            namespace: "social",
            name: "Comment",
            fields: &["target", "author_id", "social_comment_text", "created_at"],
        },
    ],
};

const FILES: ShareSpec = ShareSpec {
    namespaces: &["lastdb-files"],
    schemas: &[
        SchemaSpec {
            namespace: "lastdb-files",
            name: "FileCatalog",
            fields: &["entries"],
        },
        SchemaSpec {
            namespace: "lastdb-files",
            name: "FileChunk",
            fields: &["data"],
        },
    ],
};

fn share_spec(name: &str, version: u32) -> Result<&'static ShareSpec, PolicyError> {
    match (name, version) {
        ("lastdb-files", 1) => Ok(&FILES),
        ("social", 1) => Ok(&SOCIAL),
        ("lastdb-files" | "social", _) => Err(PolicyError::UnsupportedVersion),
        _ => Err(PolicyError::UnknownSpec),
    }
}

/// Versioned spec binding stored beside a database id, name, and kind.
///
/// This is registry contract A's `spec` object: `{ "name", "version" }`
/// with an integer version of at least 1. Catalog label `1.0` is version `1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecBinding {
    name: String,
    version: u32,
}

impl SpecBinding {
    /// Accept a syntactically valid spec name and version. Catalog membership
    /// is checked by [`Self::admitted`] and [`admit_namespace`].
    pub fn new(name: &str, version: u32) -> Result<Self, PolicyError> {
        if !valid_label(name) {
            return Err(PolicyError::InvalidName);
        }
        if version >= 1 {
            Ok(Self {
                name: name.to_owned(),
                version,
            })
        } else {
            Err(PolicyError::UnsupportedVersion)
        }
    }

    /// The versioned transport marker for a complete, explicitly shared database.
    pub fn whole_database() -> Self {
        Self {
            name: WHOLE_DATABASE.to_owned(),
            version: 1,
        }
    }

    /// Whether the binding identifies a complete database publication.
    pub fn is_whole_database(&self) -> bool {
        self.name == WHOLE_DATABASE && self.version == 1
    }

    /// Whether the name is reserved for the complete database transport.
    pub fn is_whole_database_name(name: &str) -> bool {
        name == WHOLE_DATABASE
    }

    /// Borrow the spec name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registry spec version. `1` is the current catalog.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The binding the registry, the publication marker, and the daemon share.
    ///
    /// `name` and `version` are the stored `{ "name", "version" }` object.
    /// A missing spec is not this call. An unknown catalog entry is an error.
    pub fn admitted(name: &str, version: u32) -> Result<Self, PolicyError> {
        let binding = Self::new(name, version)?;
        share_namespaces(Some(&binding))?;
        Ok(binding)
    }
}

/// Whether a namespace may enter the bound share database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceClass {
    /// The bound spec lists this namespace.
    Official,
    /// The namespace stays in `personal`.
    AppPrivate,
}

/// Why admission refused the database, binding, or namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// Share admission was asked for no binding.
    BindingAbsent,
    /// The spec name is not in the catalog.
    UnknownSpec,
    /// The spec name exists and this version does not.
    UnsupportedVersion,
    /// The database name or spec name is outside the selector alphabet.
    InvalidName,
    /// The namespace is outside the selector alphabet.
    InvalidNamespace,
}

fn share_namespaces(binding: Option<&SpecBinding>) -> Result<&'static [&'static str], PolicyError> {
    let Some(binding) = binding else {
        return Err(PolicyError::BindingAbsent);
    };
    if binding.is_whole_database() {
        Ok(&[])
    } else {
        Ok(share_spec(binding.name(), binding.version())?.namespaces)
    }
}

/// Classify one namespace for a share binding. A missing or unknown binding
/// is an error, not an allow.
pub fn admit_namespace(
    binding: Option<&SpecBinding>,
    namespace: &str,
) -> Result<NamespaceClass, PolicyError> {
    if !valid_label(namespace) {
        return Err(PolicyError::InvalidNamespace);
    }
    let namespaces = share_namespaces(binding)?;
    if binding.is_some_and(SpecBinding::is_whole_database) || namespaces.contains(&namespace) {
        Ok(NamespaceClass::Official)
    } else {
        Ok(NamespaceClass::AppPrivate)
    }
}

/// True when LastDB may publish this record on the binding.
///
/// The schema and every named field must be listed. `file` is true only when
/// the mutation carries the record's file bytes. Those bytes are eligible for
/// social `Photo` and `lastdb-files` `FileChunk`. A private schema, an unlisted
/// field, or any other file is `Ok(false)`. A bad binding is an error.
/// A missing spec is not classified here.
pub fn record_is_shareable(
    binding: &SpecBinding,
    namespace: &str,
    schema: &str,
    fields: &[&str],
    file: bool,
) -> Result<bool, PolicyError> {
    let listed = share_spec(binding.name(), binding.version())?.schemas;
    let Some(found) = listed
        .iter()
        .find(|item| item.namespace == namespace && item.name == schema)
    else {
        return Ok(false);
    };
    if fields.iter().any(|field| !found.fields.contains(field)) {
        return Ok(false);
    }
    Ok(!file
        || matches!(
            (namespace, schema),
            ("social", "Photo") | ("lastdb-files", "FileChunk")
        ))
}

fn valid_label(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
