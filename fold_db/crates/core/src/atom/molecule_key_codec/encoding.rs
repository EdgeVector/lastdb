/// How API HashKeys are encoded into storage key segments.
///
/// A node home always uses [`Self::BlindV1`] (keys from identity / 24-word
/// seed). [`Default`] stays [`Self::Plain`] so `MoleculeKeyCodec::default()` /
/// `plain()` stay unencoded library codecs. A range segment is the plain payload
/// of the molecule's typed range profile, or the API value when the codec has no
/// profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HashKeyEncoding {
    /// Storage hash segment == API HashKey (explicit opt-out).
    #[default]
    Plain,
    /// HMAC-blinded storage segment (`hk|v1` domain). Every node home.
    BlindV1,
}
