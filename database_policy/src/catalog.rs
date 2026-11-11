use crate::{PolicyError, SchemaSpec};

pub(crate) struct ShareSpec {
    pub namespaces: &'static [&'static str],
    pub schemas: &'static [SchemaSpec],
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

/// Social `Photo` carries the original file. `FileChunk` is the Files publication.
/// Other schemas, including personal ones, do not make a file eligible.
pub(crate) fn carries_file(namespace: &str, schema: &str) -> bool {
    matches!(
        (namespace, schema),
        ("social", "Photo") | ("lastdb-files", "FileChunk")
    )
}

pub(crate) fn share_spec(name: &str, version: u32) -> Result<&'static ShareSpec, PolicyError> {
    match (name, version) {
        ("lastdb-files", 1) => Ok(&FILES),
        ("social", 1) => Ok(&SOCIAL),
        ("lastdb-files" | "social", _) => Err(PolicyError::UnsupportedVersion),
        _ => Err(PolicyError::UnknownSpec),
    }
}

/// Standard schemas that stay in `personal`. This is not the `graph` database.
pub static PERSONAL_SCHEMAS: &[SchemaSpec] = &[
    SchemaSpec {
        namespace: "social-graph",
        name: "Person",
        fields: &[
            "person_id",
            "node_key",
            "contact_code",
            "display_name",
            "avatar",
            "added_at",
        ],
    },
    SchemaSpec {
        namespace: "social-graph",
        name: "Circle",
        fields: &["circle_id", "name", "created_at"],
    },
    SchemaSpec {
        namespace: "social-graph",
        name: "Membership",
        fields: &[
            "circle_id",
            "person_id",
            "role",
            "added_at",
            "added_via",
            "removed_at",
        ],
    },
    SchemaSpec {
        namespace: "social-graph",
        name: "Binding",
        fields: &["circle_id", "database_id", "role", "added_at"],
    },
];
