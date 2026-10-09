use super::DeclarativeSchemaDefinition;
use crate::KeyConfig;
use sha2::{Digest, Sha256};

/// Version of the identity-hash algorithm implemented by **this binary**.
///
/// Bump this in the same commit that changes any hash input — the segments,
/// their order, the separators, or the empty-value handling in
/// [`compute_identity_hash_parts`]. A change without a bump is exactly the
/// failure this constant exists to prevent.
///
/// **Why the current algorithm is 2, not 1.** At least two hash-input shapes
/// have been persisted on the primary, and git history dates the split:
/// Schema Service gained the `:key:` segment in `05a5691d1`, and the node did
/// not get it until `859f8f13f`. Between those commits the node computed a
/// different hash for the same keyed schema and, because
/// `load_schema_internal` recomputed unconditionally, **persisted** it over the
/// service's answer. That window is what left 43 rows on the primary carrying a
/// stale hash — keyed-readable but scan-invisible (Papercut 2026-08-06).
///
/// Version 1 is therefore "minted before stamping existed, provenance
/// unknown": it may be pre-`:key:`, pre-`app:`, or in fact the current shape.
/// The variants cannot be told apart after the fact because none of them were
/// stamped, which is the point of stamping from here on.
pub const IDENTITY_HASH_ALGO_VERSION: u32 = 2;

/// The version assumed for a row that carries an `identity_hash` but no
/// `identity_hash_algo_version` — every row written before this stamping
/// shipped. Such a row may be freely upgraded: recomputing at the current
/// version either reproduces the same hash (the row was already correct) or
/// repairs it (the row was one of the stale ones).
pub const LEGACY_IDENTITY_HASH_ALGO_VERSION: u32 = 1;

/// What a gated recompute did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityRecompute {
    /// The hash was recomputed and stamped at [`IDENTITY_HASH_ALGO_VERSION`].
    Recomputed,
    /// The carried identity was minted by a **newer** algorithm than this
    /// binary implements, so it was left exactly as it was. Callers must
    /// surface this — it means the node is older than its own data.
    RefusedDowngrade {
        /// The version stamped on the identity that was preserved.
        stored: u32,
        /// The version this binary implements.
        binary: u32,
    },
}

impl IdentityRecompute {
    /// True when a stored identity was preserved instead of being replaced.
    #[must_use]
    pub fn refused(self) -> bool {
        matches!(self, Self::RefusedDowngrade { .. })
    }
}

/// The one rule for whether a recompute may replace a stored identity.
///
/// Returns `Some(stored_version)` when the stored identity **must be kept**
/// because it was minted by an algorithm newer than this binary implements.
///
/// An old binary meeting a new hash is a loud reconcile, never a silent
/// overwrite. Before this gate existed, `load_schema_internal` recomputed and
/// persisted unconditionally, so a downgrade was indistinguishable from an
/// upgrade and left no trace.
///
/// A row with no stored hash has nothing to protect, and an unversioned row is
/// [`LEGACY_IDENTITY_HASH_ALGO_VERSION`] and may be upgraded.
#[must_use]
pub fn refuses_identity_downgrade(
    stored_hash: Option<&str>,
    stored_version: Option<u32>,
) -> Option<u32> {
    let stored = stored_version.unwrap_or(LEGACY_IDENTITY_HASH_ALGO_VERSION);
    (stored_hash.is_some_and(|h| !h.is_empty()) && stored > IDENTITY_HASH_ALGO_VERSION)
        .then_some(stored)
}

/// **The** schema identity-hash algorithm. One implementation, network-wide.
///
/// This takes primitives rather than a schema so that every schema type in the
/// monorepo can call it. `fold_db`'s `DeclarativeSchemaDefinition` is a
/// genuinely distinct struct (it carries `runtime_fields` for molecule
/// hydration), and it used to carry a byte-identical *copy* of this function
/// kept in sync by a doc comment reading "Must match Schema Service". The
/// comments drifted first and the logic drifted later: a binary computing
/// without the `:key:` segment persisted its own answer over the correct one,
/// leaving 43 stale rows on the primary. There is now nothing to keep in sync.
///
/// Identity is the readable name (`descriptive_name`, falling back to `name`),
/// then sorted and deduplicated field names, then the key layout when present,
/// optionally namespaced by `owner_app_id`:
///
/// - Same readable name + same fields + same key = same hash = dedup
/// - Same readable name + different fields = different hash = separate schemas
/// - Different readable name + same fields = different hash = separate schemas
/// - Same readable name + same fields + **different key layout** = different
///   hash = multi-key siblings (BoardCards `board` vs MilestoneCards
///   `milestone`) — never a silent `AlreadyExists` of the other pin.
///
/// Declared-field metadata (`field_hashes`, `field_versions`, and
/// `field_declarations`) deliberately does **not** participate. The schema is
/// identified before those catalog stamps arrive, so attaching or refreshing
/// them must not re-identify an already-registered schema.
///
/// `descriptive_name` is preferred over `name` because `name` may already be a
/// hash from a previous expansion; `descriptive_name` stays stable across them.
///
/// **When `owner_app_id` is `Some(_)`, it participates.** `fbrain/Concept` and
/// `kanban/Concept` produce distinct identities even when their readable names
/// and fields match (app_identity v3.1, Lane B2a).
///
/// For back-compat, schemas with `owner_app_id == None` and **no key** use the
/// original (readable name + sorted fields) scheme so existing unkeyed hashes
/// remain stable. Adding an `owner_app_id` or a key layout is a one-way door
/// for hash identity.
///
/// Preference: `preference-schema-expand-same-product-different-keys`.
#[must_use]
pub fn compute_identity_hash_parts<'a, I>(
    owner_app_id: Option<&str>,
    descriptive_name: Option<&str>,
    name: &str,
    fields: I,
    key: Option<&KeyConfig>,
) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut field_names: Vec<&str> = fields.into_iter().collect();
    field_names.sort();
    field_names.dedup();
    let combined = field_names.join(",");
    let mut hasher = Sha256::new();

    // owner_app_id, when set, prepends `app:{id}:` to the hash input. We
    // intentionally skip this prefix entirely (not just hash an empty
    // string) when owner_app_id is None so legacy schemas keep their
    // existing identity hashes — the "destructive reset" migration
    // (design doc § Migration) covers everything that does need to
    // re-hash.
    if let Some(app_id) = owner_app_id.filter(|s| !s.is_empty()) {
        hasher.update(b"app:");
        hasher.update(app_id.as_bytes());
        hasher.update(b":");
    }

    // Use the readable name (descriptive_name preferred, falls back to name)
    let readable_name = descriptive_name.filter(|s| !s.is_empty()).unwrap_or(name);
    // Always emit the `<readable_name>:` framing — even when readable_name
    // is empty — so the name and field segments stay structurally
    // distinct in the hash input. Skipping the separator on an empty
    // name folded the name and field bytes into a single unframed
    // run, which let a schema with `name = ""` and a colon-bearing
    // field (e.g. `["X:a"]`) collide with a legitimate `name = "X",
    // fields = ["a"]` schema (both produced `sha256("X:a")`).
    hasher.update(readable_name.as_bytes());
    hasher.update(b":");
    hasher.update(combined.as_bytes());

    // Key layout participates when present so multi-key siblings never
    // share an identity_hash, and so a catalog-minted keyed hash survives
    // `load_schema_internal`'s unconditional recompute. Unkeyed schemas
    // omit this segment entirely (legacy hash wire format unchanged).
    //
    // A key whose hash and range fields are both empty is indistinguishable
    // from no key at all, and emits no segment either way.
    if let Some(key) = key {
        let hf = key
            .hash_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let rf = key
            .range_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if !hf.is_empty() || !rf.is_empty() {
            hasher.update(b":key:");
            hasher.update(hf.as_bytes());
            hasher.update(b":");
            hasher.update(rf.as_bytes());
        }
    }

    format!("{:x}", hasher.finalize())
}

/// The app-namespaced canonical name for a schema. One implementation, so the
/// node and the service cannot disagree about what a schema is called.
///
/// - `owner_app_id == Some("fbrain")`, `name == "Concept"` -> `"fbrain/Concept"`.
/// - `owner_app_id == None` (or empty) -> the bare `name` (legacy behavior —
///   schemas without an owning app keep their un-namespaced name).
///
/// Inverse of [`parse_canonical_name`]. See app_identity v3.1, Lane B2a.
#[must_use]
pub fn canonical_name_parts(owner_app_id: Option<&str>, name: &str) -> String {
    match owner_app_id.filter(|s| !s.is_empty()) {
        Some(app_id) => format!("{app_id}/{name}"),
        None => name.to_string(),
    }
}

/// Split a canonical name into `(owner_app_id, name)`.
///
/// - `"fbrain/Concept"` -> `(Some("fbrain"), "Concept")`.
/// - `"Concept"` (no slash) -> `(None, "Concept")` — a legacy, un-namespaced name.
///
/// Splits on the **first** `/` only, so a schema name that itself contains
/// slashes stays intact in the returned `name`. Inverse of
/// [`canonical_name_parts`].
#[must_use]
pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
    match canonical.split_once('/') {
        Some((app_id, name)) if !app_id.is_empty() => (Some(app_id.to_string()), name),
        _ => (None, canonical),
    }
}

impl DeclarativeSchemaDefinition {
    /// Compute and store this schema's identity hash, stamping it with the
    /// algorithm version that produced it.
    ///
    /// Thin accessor glue over [`compute_identity_hash_parts`], which holds the
    /// algorithm. This is the **ungated** recompute: it always overwrites. Any
    /// path that may be handed an identity minted elsewhere should call
    /// [`Self::recompute_identity_hash_unless_newer`] instead.
    pub fn compute_identity_hash(&mut self) {
        self.identity_hash = Some(compute_identity_hash_parts(
            self.owner_app_id.as_deref(),
            self.descriptive_name.as_deref(),
            &self.name,
            self.fields
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str),
            self.key.as_ref(),
        ));
        self.identity_hash_algo_version = Some(IDENTITY_HASH_ALGO_VERSION);
    }

    /// The algorithm version that minted this schema's `identity_hash`.
    ///
    /// An unstamped row reports [`LEGACY_IDENTITY_HASH_ALGO_VERSION`].
    #[must_use]
    pub fn identity_hash_algo_version(&self) -> u32 {
        self.identity_hash_algo_version
            .unwrap_or(LEGACY_IDENTITY_HASH_ALGO_VERSION)
    }

    /// True when this schema's identity was minted by an algorithm newer than
    /// this binary implements — i.e. the node is older than its own data.
    #[must_use]
    pub fn identity_is_newer_than_binary(&self) -> bool {
        refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        )
        .is_some()
    }

    /// Recompute the identity hash **unless** the one already carried was
    /// minted by a newer algorithm, in which case it is left untouched.
    ///
    /// Thin accessor glue over [`refuses_identity_downgrade`], which holds the
    /// rule. Callers must not ignore the returned
    /// [`IdentityRecompute::RefusedDowngrade`] — it is the signal that a
    /// reconcile is needed.
    #[must_use = "a refused downgrade must be surfaced, not dropped"]
    pub fn recompute_identity_hash_unless_newer(&mut self) -> IdentityRecompute {
        if let Some(stored) = refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        ) {
            return IdentityRecompute::RefusedDowngrade {
                stored,
                binary: IDENTITY_HASH_ALGO_VERSION,
            };
        }
        self.compute_identity_hash();
        IdentityRecompute::Recomputed
    }

    /// The app-namespaced canonical name for this schema.
    ///
    /// Thin accessor glue over [`canonical_name_parts`].
    #[must_use]
    pub fn canonical_name(&self) -> String {
        canonical_name_parts(self.owner_app_id.as_deref(), &self.name)
    }

    /// Split a canonical name into `(owner_app_id, name)`.
    ///
    /// Thin accessor glue over the free [`parse_canonical_name`].
    #[must_use]
    pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
        parse_canonical_name(canonical)
    }

    /// Deduplicate the fields list in-place, preserving order.
    /// Used by schema_service when materializing/normalizing schemas.
    pub fn dedup_fields(&mut self) {
        if let Some(ref mut fields) = self.fields {
            let mut seen = std::collections::HashSet::new();
            fields.retain(|f| seen.insert(f.clone()));
        }
    }

    /// Get the identity hash
    #[must_use]
    pub fn get_identity_hash(&self) -> Option<&String> {
        self.identity_hash.as_ref()
    }
}
