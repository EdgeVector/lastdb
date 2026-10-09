//! DB handle / locator → local `storage_prefix` resolution.
//!
//! Org multi-DB cohabitation (design-org-db-handle-platform-gap): clients send
//! an explicit DB handle on every data-path request (`X-LastDB-Db`). Mini maps
//! that handle to an optional 64-hex `storage_prefix` that scopes molecule keys
//! (`{prefix}:mk:…`, `{prefix}:atom:…` when applicable).
//!
//! Locator forms (platform-shaped, no org product dependency):
//! - `lastdb://personal` / `personal` / empty → no prefix (default home)
//! - `lastdb://org/<org-slug>[/<db-slug>]` → `sha256_hex(canonical locator)`
//! - raw 64-hex → that hash as the prefix (advanced / already-resolved)
//! - `lastdb://db/<64-hex>` → same
//!
//! `db_hash` geometry matches design-cloud-sync-prefix-db-hash: stable opaque
//! id derived from the database identity (here: the canonical locator string).

use crate::hex::sha256_hex;

/// HTTP header carrying the request DB handle (locator or 64-hex).
pub const LASTDB_DB_HEADER: &str = "X-LastDB-Db";

/// Parsed DB locator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbLocator {
    /// Default personal home — no storage prefix.
    Personal,
    /// Named org DB: `lastdb://org/<org>[/<db>]`.
    Org {
        org_slug: String,
        db_slug: Option<String>,
    },
    /// Already-resolved 64 lowercase hex `db_hash`.
    DbHash(String),
}

impl DbLocator {
    /// Canonical locator string used as the hash input for org DBs.
    pub fn canonical(&self) -> String {
        match self {
            Self::Personal => "lastdb://personal".to_string(),
            Self::Org {
                org_slug,
                db_slug: None,
            } => format!("lastdb://org/{org_slug}"),
            Self::Org {
                org_slug,
                db_slug: Some(db),
            } => format!("lastdb://org/{org_slug}/{db}"),
            Self::DbHash(h) => format!("lastdb://db/{h}"),
        }
    }
}

/// Parse a client-supplied DB handle string.
///
/// Accepts the forms above; trims whitespace. Empty / missing is Personal.
pub fn parse_db_locator(raw: &str) -> Result<DbLocator, String> {
    let s = raw.trim();
    if s.is_empty() || s.eq_ignore_ascii_case("personal") || s == "lastdb://personal" {
        return Ok(DbLocator::Personal);
    }

    if let Some(rest) = s.strip_prefix("lastdb://db/") {
        return parse_db_hash(rest).map(DbLocator::DbHash);
    }

    if s.len() == 64 && is_hex(s) {
        return Ok(DbLocator::DbHash(s.to_ascii_lowercase()));
    }

    // bare org/db shorthand: edgevector/state-machine or edgevector
    if !s.starts_with("lastdb://") {
        let parts: Vec<&str> = s.split('/').filter(|p| !p.is_empty()).collect();
        return match parts.as_slice() {
            [org] => {
                assert_slug(org, "org slug")?;
                Ok(DbLocator::Org {
                    org_slug: org.to_ascii_lowercase(),
                    db_slug: None,
                })
            }
            [org, db] => {
                assert_slug(org, "org slug")?;
                assert_slug(db, "db slug")?;
                Ok(DbLocator::Org {
                    org_slug: org.to_ascii_lowercase(),
                    db_slug: Some(db.to_ascii_lowercase()),
                })
            }
            _ => Err(format!(
                "invalid DB locator: {raw:?} (use lastdb://personal or lastdb://org/<slug>[/<db>])"
            )),
        };
    }

    // lastdb://org/<org>[/<db>]
    if let Some(rest) = s.strip_prefix("lastdb://org/") {
        let parts: Vec<&str> = rest.split('/').filter(|p| !p.is_empty()).collect();
        return match parts.as_slice() {
            [org] => {
                assert_slug(org, "org slug")?;
                Ok(DbLocator::Org {
                    org_slug: org.to_ascii_lowercase(),
                    db_slug: None,
                })
            }
            [org, db] => {
                assert_slug(org, "org slug")?;
                assert_slug(db, "db slug")?;
                Ok(DbLocator::Org {
                    org_slug: org.to_ascii_lowercase(),
                    db_slug: Some(db.to_ascii_lowercase()),
                })
            }
            _ => Err(format!("invalid org DB locator: {raw:?}")),
        };
    }

    Err(format!(
        "invalid DB locator: {raw:?} (use lastdb://personal, lastdb://org/<slug>[/<db>], or 64-hex)"
    ))
}

/// Resolve a locator to the local storage prefix (None = personal home).
pub fn storage_prefix_for(locator: &DbLocator) -> Option<String> {
    match locator {
        DbLocator::Personal => None,
        DbLocator::DbHash(h) => Some(h.clone()),
        DbLocator::Org { .. } => Some(sha256_hex(locator.canonical().as_bytes())),
    }
}

/// Parse header value → `(canonical_locator, storage_prefix)`.
///
/// Empty / absent header → personal (`None` prefix). Invalid → `Err`.
pub fn resolve_db_handle_header(raw: Option<&str>) -> Result<(String, Option<String>), String> {
    let locator = match raw {
        None => DbLocator::Personal,
        Some(s) if s.trim().is_empty() => DbLocator::Personal,
        Some(s) => parse_db_locator(s)?,
    };
    let canonical = locator.canonical();
    let prefix = storage_prefix_for(&locator);
    Ok((canonical, prefix))
}

fn parse_db_hash(raw: &str) -> Result<String, String> {
    let s = raw.trim();
    if s.len() == 64 && is_hex(s) {
        Ok(s.to_ascii_lowercase())
    } else {
        Err(format!(
            "db_hash must be 64 hex characters, got len={}",
            s.len()
        ))
    }
}

fn assert_slug(slug: &str, label: &str) -> Result<(), String> {
    let ok = !slug.is_empty()
        && slug.len() <= 63
        && slug.bytes().enumerate().all(|(i, b)| {
            matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_')
                && (i > 0 || !matches!(b, b'-' | b'_'))
        });
    // first char must be alnum (matches org app)
    let first_ok = slug
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphanumeric());
    if ok && first_ok {
        Ok(())
    } else {
        Err(format!("invalid {label}: {slug:?}"))
    }
}

fn is_hex(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_hexdigit())
}
