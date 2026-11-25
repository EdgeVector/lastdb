//! Versioned whole-database spec binding and selector label validation.
//!
//! The crate reads no clock and opens no file. Whole-database shares publish
//! every schema, and member write behaviour comes from the signed grant and
//! lease chain.

const WHOLE_DATABASE: &str = "whole-database";

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
    /// Accept a syntactically valid spec name and version (`>= 1`).
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
    #[must_use]
    pub fn whole_database() -> Self {
        Self {
            name: WHOLE_DATABASE.to_owned(),
            version: 1,
        }
    }

    /// Whether the binding identifies a complete database publication.
    #[must_use]
    pub fn is_whole_database(&self) -> bool {
        self.name == WHOLE_DATABASE && self.version == 1
    }

    /// Whether the name is reserved for the complete database transport.
    #[must_use]
    pub fn is_whole_database_name(name: &str) -> bool {
        name == WHOLE_DATABASE
    }

    /// Borrow the spec name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registry spec version. `1` is the current catalog.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }
}

/// Why [`SpecBinding::new`] refused a binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// The spec version is 0.
    UnsupportedVersion,
    /// The spec name is outside the selector alphabet.
    InvalidName,
}

/// Whether `value` is 1..=64 ASCII alphanumeric, `-`, or `_` bytes.
#[must_use]
pub fn valid_label(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
