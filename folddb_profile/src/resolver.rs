//! Profile-fallback resolvers for the `folddb dev` publish verbs.
//!
//! After `folddb login` writes the active env profile to
//! `$FOLDDB_HOME/config.toml`, a developer should not have to repeat
//! `--dev-api-key` / `--schema-service-url` (or export `EXEMEM_DEV_API_KEY`)
//! on every `folddb dev schema|view|app publish`. These helpers layer the
//! profile in as the *last* fallback, preserving the override order:
//!
//! - **API key:** flag → `EXEMEM_DEV_API_KEY` (or the profile's
//!   `api_key_env`) → the active-env profile's recorded key → error.
//! - **schema_service URL:** flag → env → the active-env profile's
//!   `schema_service_url` override → `None` (the caller keeps its own
//!   built-in `DEFAULT_SCHEMA_SERVICE_URL`).
//!
//! A missing or partial profile is **not** an error — it simply yields no
//! override, mirroring the fold-side `Profile::default()` behavior (a
//! brand-new machine looks like the default profile). The only hard error is
//! the *unresolved API key* case, and its message is actionable: it points
//! the developer at `folddb login` and the flag/env fallbacks.
//!
//! Note on the API key: the profile stores only the key's env-var *name* and
//! a 6-char display tail, never the secret (see [`crate::profile`]). So the
//! profile fallback for the API key recovers a key only when the env var the
//! profile names is actually set in the developer's shell. The full secret an
//! invited developer obtained at `login` lives in `fold_db_node`'s secure
//! `DeveloperKeyStore`, which the dev node does not consult (it has its own,
//! separate, out-of-scope keypair store). In practice `folddb login` exports
//! / records `EXEMEM_DEV_API_KEY`, so flag → env covers the logged-in
//! developer; the profile layer here makes the *URL* and the env-var-name
//! override stick without re-typing.

use crate::profile::{resolve_api_key, Profile};

/// The actionable error a publish verb surfaces when no API key can be
/// resolved from flag, env, or profile. Worded to mirror the dev node's
/// existing `preflight` message but pointing at `folddb login` first.
pub const UNRESOLVED_API_KEY_MESSAGE: &str = "no dev API key found.
       The dev node's publish verbs need a developer API key to mint a
       DevCert. Resolve one of these (in precedence order):
         1. Pass --dev-api-key em_<48 hex chars>
         2. export EXEMEM_DEV_API_KEY=em_<48 hex chars>
         3. Run `folddb login --invite <code>` so the active env profile
            ($FOLDDB_HOME/config.toml) carries it for you.
       Mint a key at https://www.exemem.com/developer.";

/// Resolve the schema_service URL override from the active env profile.
///
/// Returns `None` when the profile is missing/partial (no override) or the
/// active-env table has no `schema_service_url` — the caller keeps its own
/// `DEFAULT_SCHEMA_SERVICE_URL`. A malformed profile file is the one case
/// that errors (don't silently ignore a developer's corrupt config).
///
/// `flag` is the already-resolved `--schema-service-url` value (clap folds
/// flag + any env binding into it); when `Some`, it wins and the profile is
/// not consulted.
pub fn resolve_schema_service_url(
    flag: Option<&str>,
) -> Result<Option<String>, crate::profile::ProfileError> {
    if let Some(url) = flag {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Ok(Some(url.to_string()));
        }
    }
    let profile = Profile::load()?;
    // A missing file → Profile::default() (no override). An unknown active
    // env label is a real config error and propagates.
    let env_profile = profile.active_env_profile()?;
    Ok(env_profile.schema_service_url.clone())
}

/// Resolve the dev API key from flag → env → active-env profile.
///
/// `explicit` is the already-resolved `--dev-api-key` value (clap folds the
/// flag + the `EXEMEM_DEV_API_KEY` env binding into it). When it is `None`
/// (or blank), this loads the active env profile and applies
/// [`resolve_api_key`], which honors any `api_key_env` override the profile
/// recorded and reads that env var.
///
/// A missing/partial/default profile is fine — it just means
/// [`resolve_api_key`] falls back to the default `EXEMEM_DEV_API_KEY` var.
/// A malformed profile file errors (surfaced as a `ProfileError`).
pub fn resolve_dev_api_key(
    explicit: Option<&str>,
) -> Result<Option<String>, crate::profile::ProfileError> {
    let profile = Profile::load()?;
    let env_profile = profile.active_env_profile()?;
    Ok(resolve_api_key(env_profile, explicit))
}
