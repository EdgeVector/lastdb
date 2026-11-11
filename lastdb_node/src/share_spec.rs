//! Share-create gate over `database_policy`. A missing new binding is a denial.
//!
//! A version-1 publication marker with no `spec` object is a legacy publication.
//! That absence is not a catalog binding and is not treated as `lastdb-files`.
use database_policy::{admit_share, PolicyError, SpecBinding};
use serde::{Deserialize, Serialize};

/// Registry `spec` object stored on a new publication marker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationSpec {
    /// Spec name. This is not a database name.
    pub name: String,
    /// Registry version. Catalog label `1.0` is `1`.
    pub version: u32,
}

pub(crate) fn require_new(name: &str, version: u32) -> Result<SpecBinding, String> {
    let binding = SpecBinding::new(name, version).map_err(text)?;
    admit_share(Some(&binding)).map_err(text)?;
    Ok(binding)
}

/// `None` is a legacy marker. It is not passed to [`admit_share`].
pub(crate) fn resolve_stored(
    name: Option<&str>,
    version: Option<u32>,
) -> Result<Option<SpecBinding>, String> {
    match (name, version) {
        (None, None) => Ok(None),
        (Some(name), Some(version)) => Ok(Some(require_new(name, version)?)),
        _ => Err("share publication spec binding is incomplete".into()),
    }
}

/// Existing files homes have no spec object. A bound home must be lastdb-files.
pub(crate) fn require_files(name: Option<&str>, version: Option<u32>) -> Result<(), String> {
    match resolve_stored(name, version)? {
        None => Ok(()),
        Some(binding) if binding == SpecBinding::legacy_files_publication() => Ok(()),
        Some(_) => Err("file sharing requires the lastdb-files publication spec".into()),
    }
}

fn text(error: PolicyError) -> String {
    error.as_str().to_owned()
}
