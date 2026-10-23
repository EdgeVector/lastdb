/// How API HashKeys are encoded into storage key segments.
///
/// **Product default (env unset): [`Self::BlindV1`]** so fresh Mini installs
/// write blinded keys from first boot (keys from identity / 24-word seed).
/// Explicit `plain` remains for rare unencoded homes.
/// There is **no** dual-read / mid-flight `migrating_*` mode — one encoding,
/// one path.
///
/// [`Default`] for this enum stays [`Self::Plain`] so
/// `MoleculeKeyCodec::default()` / `plain()` stay unencoded without env.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashKeyEncoding {
    /// Storage hash segment == API HashKey (explicit opt-out).
    #[default]
    Plain,
    /// HMAC-blinded storage segment (`hk|v1` domain). Product default via env.
    BlindV1,
}

impl HashKeyEncoding {
    /// Parse `LASTDB_HASH_KEY_ENCODING` (`plain` | `blind_v1`).
    ///
    /// - **Unset** → [`Self::BlindV1`] (fresh install: encrypt keys from start)
    /// - `plain` → [`Self::Plain`]
    /// - `blind_v1` → [`Self::BlindV1`]
    #[must_use]
    pub fn from_env_or_default() -> Self {
        match std::env::var("LASTDB_HASH_KEY_ENCODING") {
            Ok(s) if s.eq_ignore_ascii_case("plain") => Self::Plain,
            Ok(s) if s.eq_ignore_ascii_case("blind_v1") => Self::BlindV1,
            Ok(s) => {
                tracing::warn!(
                    encoding = %s,
                    "unknown LASTDB_HASH_KEY_ENCODING; using blind_v1"
                );
                Self::BlindV1
            }
            Err(_) => Self::BlindV1,
        }
    }
}

/// How API RangeKeys are encoded into storage range segments.
///
/// **Product default (env unset): [`Self::OpeV1`]** so fresh Mini installs
/// write OPE ranges from first boot. Order leakage under OPE is accepted
/// (design-lastdb-rangekey-ope-v1). Per-byte OPE **is** byte-prefix preserving
/// (`ope(prefix)` is a string prefix of `ope(prefix||suffix)`), so RangePrefix
/// / HashRangePrefix work when scan *and* in-memory apply use storage-form
/// bounds (see `filter_range_bounds_for_apply`).
///
/// Explicit `plain` remains a supported unencoded placement choice.
///
/// [`Default`] for this enum stays [`Self::Plain`] for library `plain()` codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RangeKeyEncoding {
    /// Storage range segment == API RangeKey (tests / explicit opt-out).
    #[default]
    Plain,
    /// Order-preserving opaque encoding (`rk|ope|v1`). Product default via env.
    OpeV1,
}

impl RangeKeyEncoding {
    /// Parse `LASTDB_RANGE_KEY_ENCODING` (`plain` | `ope_v1`).
    ///
    /// - **Unset** → [`Self::OpeV1`] (fresh install: encode ranges from start)
    /// - `plain` → [`Self::Plain`]
    /// - `ope_v1` → [`Self::OpeV1`]
    #[must_use]
    pub fn from_env_or_default() -> Self {
        match std::env::var("LASTDB_RANGE_KEY_ENCODING") {
            Ok(s) if s.eq_ignore_ascii_case("plain") => Self::Plain,
            Ok(s) if s.eq_ignore_ascii_case("ope_v1") => Self::OpeV1,
            Ok(s) => {
                tracing::warn!(
                    encoding = %s,
                    "unknown LASTDB_RANGE_KEY_ENCODING; using ope_v1"
                );
                Self::OpeV1
            }
            Err(_) => Self::OpeV1,
        }
    }

    #[must_use]
    pub const fn writes_ope(self) -> bool {
        matches!(self, Self::OpeV1)
    }
}
