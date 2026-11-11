//! Legacy share creation and explicit spec checks.
//!
//! `lastdb share create --into` with no spec stays on the legacy path.
//! That absence is not a catalog binding and is not `lastdb-files`.
//! An explicit spec is validated before a home exists. Named-database
//! admission stays fail-closed through `database_policy::admit_share`.
use database_policy::{admit_share, PolicyError, SpecBinding};
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

/// No spec keeps the legacy path. A name without a version uses version `1`.
/// A version without a name is incomplete. An explicit spec must be admitted.
pub(crate) fn binding_for_create(
    name: Option<&str>,
    version: Option<u32>,
) -> Result<Option<SpecBinding>, String> {
    match (name, version) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err("share publication spec binding is incomplete".into()),
        (Some(name), version) => Ok(Some(require_new(name, version.unwrap_or(1))?)),
    }
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

/// The Files import command. A marker with no spec stays on that legacy
/// command and is not the `lastdb-files` spec. Any other explicit spec is
/// refused here; a social `Photo` file uses `file_is_shareable` instead.
pub(crate) fn require_files(name: Option<&str>, version: Option<u32>) -> Result<(), String> {
    match resolve_stored(name, version)? {
        None => Ok(()),
        Some(binding) if binding == SpecBinding::legacy_files_publication() => Ok(()),
        Some(_) => Err("share files import does not accept this spec".into()),
    }
}

fn text(error: PolicyError) -> String {
    error.as_str().to_owned()
}
