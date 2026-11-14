//! Registry `spec` metadata carried on share invitations and markers.
use serde::{Deserialize, Serialize};

/// Registry `spec` object stored on a publication marker that named one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationSpec {
    /// Spec name. This is not a database name.
    pub name: String,
    /// Registry version. Catalog label `1.0` is `1`.
    pub version: u32,
}
