//! Centralized endpoint registry for Exemem services.
//!
//! All cross-environment URLs live in [`environments.json`] — that is the
//! **single source of truth**. The file lives in THIS crate
//! (`folddb_profile/environments.json`; it moved here when the desktop
//! `fold_db_node` crate was deleted in the Mini-only cutover, 2026-07-12);
//! this crate's `build.rs` parses it at compile time and emits per-(env,
//! key) constants in `OUT_DIR`, included below via `gen::*`. Edit URLs in
//! `environments.json`; do NOT add hardcoded gateway hostnames in Rust,
//! shell, or config files. `scripts/lints/lint-no-hardcoded-urls.sh`
//! enforces this in CI.
//!
//! This module used to live in `fold_db_node::endpoints`. The desktop node
//! was removed in the Mini-only cutover. The profile loader and Mini daemon
//! now use this crate directly.
//!
//! The active environment is selected by `ENVIRONMENT` (the var CDK sets on
//! deployed Lambdas), falling back to `EXEMEM_ENV` for local CLI use. The
//! Mini daemon can also pin the environment from its cloud connection.
//! Per-call overrides still work (`FOLD_SCHEMA_SERVICE_URL`,
//! `EXEMEM_API_URL`) for ad-hoc testing.

mod gen {
    include!(concat!(env!("OUT_DIR"), "/environments_generated.rs"));
}

use std::sync::OnceLock;

/// Service environment — dev or prod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Dev,
    Prod,
}

/// The `ENVIRONMENT` / `EXEMEM_ENV` answer, parsed once per process.
/// `None` = neither var is set.
static EXPLICIT_ENV: OnceLock<Option<Environment>> = OnceLock::new();

/// The environment of the node home's persisted cloud connection
/// (`lastdb connect --env <e>` writes that env's Exemem `api_url` into
/// `<home>/cloud_sync.json`). The daemon pins it at boot with
/// [`set_home_environment`]; it outranks the build-profile default but never
/// an explicit env var.
static HOME_ENV: OnceLock<Environment> = OnceLock::new();

/// Pin the environment derived from the node home's cloud connection.
///
/// Called once by the daemon at boot. Returns `false` (and keeps the first
/// value) when a different environment was already pinned.
pub fn set_home_environment(env: Environment) -> bool {
    *HOME_ENV.get_or_init(|| env) == env
}

/// The environment pinned by [`set_home_environment`], if any.
pub fn home_environment() -> Option<Environment> {
    HOME_ENV.get().copied()
}

/// Map a persisted Exemem API URL back to its environment. `None` for a
/// custom URL that is neither registry entry.
pub fn environment_for_exemem_api_url(api_url: &str) -> Option<Environment> {
    let url = api_url.trim().trim_end_matches('/');
    [Environment::Dev, Environment::Prod]
        .into_iter()
        .find(|env| exemem_api_url_for(*env).trim_end_matches('/') == url)
}

/// Raw value didn't parse as a known environment name.
#[derive(Debug, thiserror::Error)]
#[error("unknown environment value: {0:?} (expected \"dev\" or \"prod\")")]
pub struct UnknownEnvironment(pub String);

impl Environment {
    /// Resolve from `ENVIRONMENT` (Lambda/CDK), then `EXEMEM_ENV` (local CLI).
    /// When neither is set, the Mini daemon's pinned cloud environment wins.
    /// Otherwise, the build profile sets the default: debug → Dev and
    /// release → Prod.
    ///
    /// The env-var parse and its log line run once per process. Each call
    /// then applies the pinned home environment, if one exists. This cache
    /// keeps repeated endpoint lookups from logging the same message.
    ///
    /// This is the lenient, always-succeeds entry point. Callers that must
    /// not silently default (e.g. a Lambda cold-start deciding whether a
    /// dev-only fallback secret is allowed) should use
    /// [`Environment::require_from_env`] instead.
    pub fn from_env() -> Self {
        Self::pick(
            Self::explicit_from_env(),
            home_environment(),
            Self::build_default(),
        )
    }

    /// The `ENVIRONMENT` / `EXEMEM_ENV` answer only (`None` when neither is
    /// set). The daemon compares it with the home's cloud environment at boot.
    pub fn explicit_from_env() -> Option<Self> {
        *EXPLICIT_ENV.get_or_init(Self::resolve_explicit_once)
    }

    /// Resolution order, as a pure function: an explicit env var wins, then
    /// the node home's cloud environment, then the build-profile default.
    pub fn pick(explicit: Option<Self>, home: Option<Self>, default: Self) -> Self {
        explicit.or(home).unwrap_or(default)
    }

    /// Debug builds default to Dev (so `cargo run` stays on us-west-2);
    /// release builds default to Prod (the shipped binary hits us-east-1).
    pub fn build_default() -> Self {
        if cfg!(debug_assertions) {
            Self::Dev
        } else {
            Self::Prod
        }
    }

    /// Parse a raw env-var value with no env/global-state access — used for
    /// testability and by callers that need to preserve their own
    /// unset/unparseable fallback behavior instead of this module's default.
    pub fn try_from_str(raw: &str) -> Result<Self, UnknownEnvironment> {
        match raw {
            "prod" | "production" => Ok(Self::Prod),
            "dev" | "development" => Ok(Self::Dev),
            other => Err(UnknownEnvironment(other.to_string())),
        }
    }

    /// Fail-closed resolution of `var_name` for security-sensitive cold-start
    /// checks: panics with a clear message when the var is unset, empty, or
    /// unrecognized — no silent default. Does not use the process-wide cache,
    /// so it's safe to call from a crate that reads a different var name than
    /// `from_env()`'s `ENVIRONMENT`/`EXEMEM_ENV` pair.
    pub fn require_from_env(var_name: &str) -> Self {
        match std::env::var(var_name) {
            Ok(v) if !v.is_empty() => Self::try_from_str(&v).unwrap_or_else(|_| {
                panic!(
                    "CRITICAL: {var_name} env var has unrecognized value {v:?} \
                     (expected 'dev' or 'prod')."
                )
            }),
            _ => panic!(
                "CRITICAL: {var_name} env var must be set (expected 'dev' or 'prod'). \
                 Refusing to start."
            ),
        }
    }

    /// Lowercase name (`"dev"` / `"prod"`) — for logging and for embedding
    /// in externally-visible artifacts (e.g. a signed cert's `env` field)
    /// where the exact literal matters, not just `Debug`'s `Dev`/`Prod`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }

    fn resolve_explicit_once() -> Option<Self> {
        let raw = std::env::var("ENVIRONMENT")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var("EXEMEM_ENV").ok());
        match raw.as_deref() {
            Some("prod" | "production") => Some(Self::Prod),
            Some("dev" | "development") => Some(Self::Dev),
            Some(other) => {
                tracing::error!(
                    "ENVIRONMENT/EXEMEM_ENV has unknown value '{}', defaulting to dev",
                    other
                );
                Some(Self::Dev)
            }
            None => {
                tracing::info!(
                    "ENVIRONMENT/EXEMEM_ENV not set; using the node home's cloud \
                     environment if pinned, else the build default ({})",
                    Self::build_default()
                );
                None
            }
        }
    }
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn service_url(env_var: &str, dev: &'static str, prod: &'static str) -> String {
    std::env::var(env_var).unwrap_or_else(|_| match Environment::from_env() {
        Environment::Dev => dev.to_string(),
        Environment::Prod => prod.to_string(),
    })
}

/// Schema service URL: `FOLD_SCHEMA_SERVICE_URL`, else the URL of
/// [`Environment::from_env`] (env var > node home cloud env > build default).
pub fn schema_service_url() -> String {
    schema_service_url_from(
        std::env::var("FOLD_SCHEMA_SERVICE_URL").ok(),
        Environment::from_env(),
    )
}

/// Pure form of [`schema_service_url`]: an explicit override URL wins over
/// the environment's registry URL.
pub fn schema_service_url_from(override_url: Option<String>, env: Environment) -> String {
    override_url
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| schema_service_url_for(env).to_string())
}

/// Exemem API URL (auth, sync, etc.).
pub fn exemem_api_url() -> String {
    service_url("EXEMEM_API_URL", gen::DEV_EXEMEM_API, gen::PROD_EXEMEM_API)
}

/// Schema service URL for a specific environment, ignoring `EXEMEM_ENV`.
/// Used by tooling (e.g. the daemon launcher's `--dev` flag) that needs to
/// pin the dev URL regardless of the calling process's env.
pub fn schema_service_url_for(env: Environment) -> &'static str {
    match env {
        Environment::Dev => gen::DEV_SCHEMA_SERVICE,
        Environment::Prod => gen::PROD_SCHEMA_SERVICE,
    }
}

/// Exemem API URL for a specific environment, ignoring `EXEMEM_ENV`.
/// Used by the `folddb` developer CLI so `--env prod` doesn't silently
/// inherit `EXEMEM_ENV=dev` from the developer's shell.
pub fn exemem_api_url_for(env: Environment) -> &'static str {
    match env {
        Environment::Dev => gen::DEV_EXEMEM_API,
        Environment::Prod => gen::PROD_EXEMEM_API,
    }
}

/// Web-portal base URL for a specific environment (the passkey/magic-link
/// login + developer API-key dashboard). The dev and prod portal URLs are
/// the `portal` entries in `environments.json` (dev = the `ExememStack-dev`
/// CloudFront distribution; prod = the public site). Used by `folddb login`
/// to point a developer at the *real* key page (`{portal}/developer`) for
/// the env they're logging into — keys are per-env, so the URL must be too.
pub fn portal_url_for(env: Environment) -> &'static str {
    match env {
        Environment::Dev => gen::DEV_PORTAL,
        Environment::Prod => gen::PROD_PORTAL,
    }
}
