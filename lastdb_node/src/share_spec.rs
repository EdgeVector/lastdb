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

impl PublicationSpec {
    pub(crate) fn whole_database(&self) -> bool {
        database_policy::SpecBinding::new(&self.name, self.version)
            .is_ok_and(|binding| binding.is_whole_database())
    }
}
