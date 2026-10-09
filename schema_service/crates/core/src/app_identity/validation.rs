//! Syntactic validation of app ids, metadata, versions, code signatures,
//! sources, artifacts and `uses` lists.

use super::*;

/// `app_id` regex per design: `^[a-z][a-z0-9-]{0,39}$`.
pub(super) const APP_ID_MAX_LEN: usize = 40;
/// Per-developer app-registration quota (Tom 2026-06-11, app-isolation
/// LOW-cleanup item 5): a single `owner_dev_pubkey` may own at most this
/// many registered apps. New registrations beyond the cap are rejected
/// with HTTP 429 `quota_exceeded`; updates (`PUT /v1/apps/{app_id}`) and
/// idempotent re-posts of an already-owned app never count against or
/// trip the quota. A named const so raising the cap is a one-line change.
pub const MAX_APPS_PER_DEVELOPER: usize = 25;
pub(super) const META_DISPLAY_NAME_MAX: usize = 80;
pub(super) const META_DESCRIPTION_MAX: usize = 500;
pub(super) const META_URL_MAX: usize = 200;
/// Total metadata blob bound after JCS canonicalization (design: < 2 KB).
pub(super) const META_TOTAL_MAX_BYTES: usize = 2048;
/// macOS bundle identifiers are short reverse-DNS strings; 200 chars is a
/// generous ceiling (same bound as the metadata URLs).
pub(super) const CODE_SIG_BUNDLE_ID_MAX: usize = 200;
/// Apple Developer ID team identifiers are exactly 10 alphanumerics.
pub(super) const CODE_SIG_TEAM_ID_LEN: usize = 10;
/// Upper bound on how many cross-app schemas/outputs an app may declare in
/// its manifest `[uses]` list. A declarative intent surface, not a security
/// boundary — but bounded so a runaway manifest can't bloat the registry
/// snapshot every node mirrors. Generous for any realistic app.
pub(super) const USES_MAX_ENTRIES: usize = 64;
/// Max chars for one `[uses]` entry (a canonical `app/Schema` name). A
/// canonical name is two short ids joined by `/`; 80 is generous and well
/// under the metadata-URL ceiling.
pub(super) const USES_ENTRY_MAX_LEN: usize = 80;
pub(super) const APP_SOURCE_MAX_LEN: usize = 300;
pub(super) const APP_ARTIFACT_HASH_MAX_LEN: usize = 128;

/// Validate an `app_id` against `^[a-z][a-z0-9-]{0,39}$` without pulling
/// in a regex engine — the grammar is small enough to check by hand.
pub fn is_valid_app_id(app_id: &str) -> bool {
    let bytes = app_id.as_bytes();
    if bytes.is_empty() || bytes.len() > APP_ID_MAX_LEN {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Registration-time charset guard for `app_id` (Tom 2026-06-11,
/// app-isolation LOW-cleanup item 4): reject an `app_id` carrying any
/// non-ASCII character or ASCII control character, with a discriminated
/// reason naming the offending code point (surfaced in the 400 body as
/// `invalid_app_id_charset`).
///
/// ## Why this guard exists (and what it deliberately does NOT do)
///
/// Downstream, `app_id` is a **security principal** compared with raw
/// exact-byte `String` equality at every ACL/capability boundary
/// (`fold_db::app_isolation::NamespaceAcl::is_governed`,
/// `WriteScope::resolve_owner` — pinned by the
/// `exact_byte_id_semantics_*` invariant tests in fold_db core). No
/// Unicode normalization or case folding is ever applied there: folding
/// would silently merge distinct principals. The anti-confusable defense
/// therefore belongs HERE, at registration time — a homoglyph or
/// NFC/NFD-variant id (e.g. `café` vs `café`) must never enter the
/// registry in the first place, because once registered it would be a
/// byte-distinct principal indistinguishable to a human reader.
///
/// Deliberately NOT rejected here: mixed/upper case. Ids are compared
/// exact-byte downstream, and the case grammar is already owned by
/// [`is_valid_app_id`] (`^[a-z][a-z0-9-]{0,39}$`). This guard is
/// registration-time only — it is never applied retroactively to reads
/// of already-registered records.
pub fn validate_app_id_charset(app_id: &str) -> Result<(), String> {
    for ch in app_id.chars() {
        if !ch.is_ascii() {
            return Err(format!(
                "app_id contains a non-ASCII character (U+{:04X}); app ids are \
                 exact-byte security principals, so visually-confusable Unicode \
                 ids are rejected at registration",
                ch as u32
            ));
        }
        if ch.is_ascii_control() {
            return Err(format!(
                "app_id contains an ASCII control character (U+{:04X}); \
                 control characters are rejected at registration",
                ch as u32
            ));
        }
    }
    Ok(())
}

/// Reject a metadata string that carries C0/C1 control characters, which
/// the owner-facing consent prompt renders as plain text straight to the
/// terminal. An ESC (`0x1b`) or cursor-movement / line-clear sequence in a
/// `display_name` would *execute* in the owner's terminal when they run
/// `folddb consent grant <app>`, letting a malicious developer erase or
/// redraw the UNVERIFIED / tier / pubkey trust lines — forging the exact
/// signals the prompt exists to convey (2026-06-10 security review, dim5
/// F2). `allow_newline` is `true` only for the multi-line `description`;
/// single-line fields (`display_name`, the URLs) reject every control
/// char including CR/LF. Plain ASCII space (`0x20`) is always allowed; it
/// is not a control character. Returns a human-readable reason naming the
/// field on the first violation.
fn reject_control_chars(field: &str, value: &str, allow_newline: bool) -> Result<(), String> {
    for ch in value.chars() {
        // C0 controls (U+0000..=U+001F, includes ESC/CR/TAB), DEL (U+007F),
        // and C1 controls (U+0080..=U+009F). Newline is conditionally
        // allowed for multi-line free text; nothing else in these ranges is.
        let is_control = ch.is_control() || ('\u{0080}'..='\u{009f}').contains(&ch);
        if is_control && !(allow_newline && ch == '\n') {
            return Err(format!(
                "{field} contains a disallowed control character (U+{:04X}); \
                 control/escape sequences are rejected so the consent prompt \
                 renders as safe plain text",
                ch as u32
            ));
        }
    }
    Ok(())
}

/// Enforce the strict metadata schema. Returns a human-readable field
/// reason on the first violation (surfaced in the 400 body).
pub fn validate_metadata(metadata: &AppMetadata) -> Result<(), String> {
    if metadata.display_name.trim().is_empty() {
        return Err("display_name is required".to_string());
    }
    if metadata.display_name.chars().count() > META_DISPLAY_NAME_MAX {
        return Err(format!(
            "display_name exceeds {META_DISPLAY_NAME_MAX} characters"
        ));
    }
    if metadata.description.chars().count() > META_DESCRIPTION_MAX {
        return Err(format!(
            "description exceeds {META_DESCRIPTION_MAX} characters"
        ));
    }
    if metadata.homepage_url.chars().count() > META_URL_MAX {
        return Err(format!("homepage_url exceeds {META_URL_MAX} characters"));
    }
    if let Some(icon_url) = &metadata.icon_url {
        if icon_url.chars().count() > META_URL_MAX {
            return Err(format!("icon_url exceeds {META_URL_MAX} characters"));
        }
    }
    // Control-char gate: every field that is (or can be) rendered into the
    // owner's terminal by the consent prompt must be free of ANSI/escape
    // injection. `display_name` and both URLs are single-line prompt fields
    // (no newline either); `description` is the only multi-line free-text
    // field, so it may carry `\n` but never ESC/CR/other C0/C1.
    reject_control_chars("display_name", &metadata.display_name, false)?;
    reject_control_chars("description", &metadata.description, true)?;
    reject_control_chars("homepage_url", &metadata.homepage_url, false)?;
    if let Some(icon_url) = &metadata.icon_url {
        reject_control_chars("icon_url", icon_url, false)?;
    }
    // Total-blob bound, measured on the canonical bytes the snapshot ships.
    let canonical = serde_json::to_value(metadata)
        .ok()
        .and_then(|v| canonicalize(&v).ok())
        .ok_or_else(|| "metadata is not serializable".to_string())?;
    if canonical.len() > META_TOTAL_MAX_BYTES {
        return Err(format!(
            "metadata exceeds {META_TOTAL_MAX_BYTES} bytes after canonicalization"
        ));
    }
    Ok(())
}

pub fn validate_app_version(version: &str) -> Result<(), String> {
    let trimmed = version.trim();
    if trimmed != version || trimmed.is_empty() {
        return Err(
            "version must be a non-empty SemVer string without surrounding whitespace".into(),
        );
    }
    semver::Version::parse(trimmed).map_err(|e| format!("version must be valid SemVer: {e}"))?;
    Ok(())
}

pub fn app_version_is_greater(candidate: &str, existing: &str) -> Result<bool, String> {
    let candidate = semver::Version::parse(candidate)
        .map_err(|e| format!("candidate version must be valid SemVer: {e}"))?;
    let existing = semver::Version::parse(existing)
        .map_err(|e| format!("existing version is invalid: {e}"))?;
    Ok(candidate > existing)
}

/// Enforce the code-signature bounds: a bundle identifier matching the
/// reverse-DNS-ish grammar `^[A-Za-z0-9.-]+$` (non-empty, at most
/// [`CODE_SIG_BUNDLE_ID_MAX`] chars), and — when present — a team id of exactly
/// [`CODE_SIG_TEAM_ID_LEN`] ASCII alphanumerics (Apple's Developer ID team
/// identifier shape). Returns a human-readable reason on the first violation
/// (surfaced in the 400 body as `invalid_code_signature`).
///
/// ## Why the grammar, not just a length bound
///
/// The bundle identifier is interpolated, quoted, into the macOS Code Signing
/// Requirement Language string every node builds and feeds to
/// `SecRequirementCreateWithString`
/// (`fold_db::access::code_signature::designated_requirement`):
/// `identifier "{bundle_identifier}" and anchor apple generic`. A value
/// containing a `"` (or `\`, or requirement-language operators) could close the
/// quoted token early and graft attacker-chosen clauses onto the predicate —
/// e.g. `x" or anchor trusted or identifier "y` — weakening the signature check
/// on every node that caches the published record. Constraining the publish-time
/// value to `[A-Za-z0-9.-]` makes that injection impossible at the source. The
/// core builder additionally fails closed on the same grammar
/// (belt-and-suspenders), and the team id is already alphanumeric-only and so
/// cannot inject.
pub fn validate_code_signature(cs: &AppCodeSignature) -> Result<(), String> {
    if cs.bundle_identifier.is_empty() {
        return Err("bundle_identifier is required".to_string());
    }
    if cs.bundle_identifier.chars().count() > CODE_SIG_BUNDLE_ID_MAX {
        return Err(format!(
            "bundle_identifier exceeds {CODE_SIG_BUNDLE_ID_MAX} characters"
        ));
    }
    if !cs
        .bundle_identifier
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
    {
        return Err(
            "bundle_identifier must match ^[A-Za-z0-9.-]+$ (reverse-DNS form; no quotes, \
             backslashes, whitespace, or control characters)"
                .to_string(),
        );
    }
    if let Some(team_id) = &cs.team_id {
        if team_id.len() != CODE_SIG_TEAM_ID_LEN
            || !team_id.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(format!(
                "team_id must be exactly {CODE_SIG_TEAM_ID_LEN} ASCII alphanumeric characters"
            ));
        }
    }
    Ok(())
}

/// Validate the optional app source checkout pointer. Install is source-first
/// in Phase 2, so the registry accepts LastGit remotes plus ordinary Git-over
/// HTTP(S)/SSH forms while rejecting empty strings and terminal control chars.
pub fn validate_app_source(source: &str) -> Result<(), String> {
    if source.trim().is_empty() {
        return Err("source must be non-empty when present".to_string());
    }
    if source.chars().count() > APP_SOURCE_MAX_LEN {
        return Err(format!("source exceeds {APP_SOURCE_MAX_LEN} characters"));
    }
    reject_control_chars("source", source, false)?;
    let allowed = source.starts_with("lastdb:///")
        || source.starts_with("https://")
        || source.starts_with("http://")
        || source.starts_with("ssh://")
        || source.starts_with("git@");
    if !allowed {
        return Err("source must be a lastdb:///, http(s), ssh, or git@ checkout URL".to_string());
    }
    Ok(())
}

/// Validate the optional signed tarball pointer. The object lives outside the
/// registry; this gate only bounds the metadata that every node mirrors.
pub fn validate_app_artifact(artifact: &AppArtifact) -> Result<(), String> {
    if artifact.hash.trim().is_empty() {
        return Err("artifact.hash must be non-empty".to_string());
    }
    if artifact.hash.chars().count() > APP_ARTIFACT_HASH_MAX_LEN {
        return Err(format!(
            "artifact.hash exceeds {APP_ARTIFACT_HASH_MAX_LEN} characters"
        ));
    }
    reject_control_chars("artifact.hash", &artifact.hash, false)?;
    if !artifact
        .hash
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b':' || b == b'.')
    {
        return Err(
            "artifact.hash may contain only ASCII letters, digits, '-', '_', ':' or '.'"
                .to_string(),
        );
    }
    if artifact.size_bytes == 0 {
        return Err("artifact.size_bytes must be greater than zero".to_string());
    }
    Ok(())
}

/// Enforce bounds on the manifest `[uses]` declaration (the cross-app
/// schemas/outputs an app declares it consumes — see [`AppRecord::uses`]).
/// Returns a human-readable reason on the first violation (surfaced in the
/// 400 body as `invalid_uses`).
///
/// Each entry must be a non-empty, control-character-free string of at most
/// [`USES_ENTRY_MAX_LEN`] chars, the whole list capped at
/// [`USES_MAX_ENTRIES`], with no duplicates. This is **intent**, not an ACL,
/// so the entry is NOT required to resolve to a registered canonical here —
/// resolution is lazy at consent/search time (an entry naming a schema that
/// does not exist yet simply never gets granted/ranked). The bounds exist
/// only to keep the snapshot every node mirrors from bloating, and the
/// control-char guard keeps a declared name from injecting into the
/// consent prompt that renders it.
pub fn validate_uses(uses: &[String]) -> Result<(), String> {
    if uses.len() > USES_MAX_ENTRIES {
        return Err(format!(
            "uses declares {} entries; the maximum is {USES_MAX_ENTRIES}",
            uses.len()
        ));
    }
    let mut seen: HashSet<&str> = HashSet::with_capacity(uses.len());
    for entry in uses {
        if entry.is_empty() {
            return Err("uses entries must be non-empty canonical names".to_string());
        }
        if entry.chars().count() > USES_ENTRY_MAX_LEN {
            return Err(format!(
                "uses entry {entry:?} exceeds {USES_ENTRY_MAX_LEN} characters"
            ));
        }
        if entry.chars().any(char::is_control) {
            return Err(format!("uses entry {entry:?} contains a control character"));
        }
        if !seen.insert(entry.as_str()) {
            return Err(format!("uses declares {entry:?} more than once"));
        }
    }
    Ok(())
}
