/// How API HashKeys are encoded into storage key segments.
///
/// A node home always uses [`Self::BlindV1`] (keys from identity / 24-word
/// seed). [`Default`] stays [`Self::Plain`] so `MoleculeKeyCodec::default()` /
/// `plain()` stay unencoded library codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HashKeyEncoding {
    /// Storage hash segment == API HashKey (explicit opt-out).
    #[default]
    Plain,
    /// HMAC-blinded storage segment (`hk|v1` domain). Every node home.
    BlindV1,
}

/// How API RangeKeys are encoded into storage range segments.
///
/// A node home uses [`Self::Plain`]; the typed molecule contract orders the
/// range bytes. [`Default`] stays [`Self::Plain`] for library `plain()` codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeKeyEncoding {
    /// Storage range segment == API RangeKey (tests / explicit opt-out).
    #[default]
    Plain,
    /// Order-preserving opaque encoding (`rk|ope|v1`).
    OpeV1,
}

impl RangeKeyEncoding {
    #[must_use]
    pub const fn writes_ope(self) -> bool {
        matches!(self, Self::OpeV1)
    }
}
