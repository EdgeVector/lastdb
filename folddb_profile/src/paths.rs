//! LastDB / FoldDB node-home path resolution + tilde expansion.
//!
//! A single resolver controls where all instance-specific state lives. As of
//! the FoldDB→LastDB rename (Phase 1) the resolution is **non-destructive and
//! honor-both**: new installs land in `~/.lastdb`, while existing installs keep
//! reading their `~/.folddb` (or whatever `FOLDDB_HOME`/`LASTDB_HOME` points
//! at) in place with ZERO data movement. See [`folddb_home`].
//!
//! This family used to live in `fold_db_node::utils::paths`; the node-home
//! resolution + tilde-expansion helpers moved here so the profile loader (and
//! the `folddb dev` dev node) can find the profile file without depending on
//! fold_db_node. `fold_db_node::utils::paths` re-exports these and keeps its
//! node-specific helpers (`observability_log_path`, `onboarding_marker_path`)
//! local.

use std::path::{Path, PathBuf};

/// Directory name for a fresh (new-install) node home.
pub const LASTDB_DIR: &str = ".lastdb";
/// Directory name for a legacy node home (pre-rename installs).
pub const FOLDDB_DIR: &str = ".folddb";
/// Canonical node-home override.
pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";
/// Legacy node-home override; still honored after [`LASTDB_HOME_ENV`].
pub const FOLDDB_HOME_ENV: &str = "FOLDDB_HOME";
/// Canonical local-node socket override.
pub const FOLDDB_SOCKET_PATH_ENV: &str = "FOLDDB_SOCKET_PATH";
/// Deprecated socket override alias; honored after [`FOLDDB_SOCKET_PATH_ENV`].
pub const FOLDDB_SOCK_ENV: &str = "FOLDDB_SOCK";
/// File name of the node's Unix-domain socket within its data dir.
pub const NODE_SOCKET_FILE_NAME: &str = "folddb.sock";

/// Which rule produced a node socket path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSocketPathSource {
    /// The canonical `FOLDDB_SOCKET_PATH` override was set.
    CanonicalOverride,
    /// The deprecated `FOLDDB_SOCK` alias was set.
    LegacyOverride,
    /// The default `<node-home>/data/folddb.sock` path was used.
    NodeHomeDefault,
}

/// Resolve the node-home directory.
///
/// Priority (the FoldDB→LastDB honor-both resolution order — explicitly
/// non-destructive: this resolver NEVER moves or copies anyone's data, it only
/// *chooses which existing directory to read*, or names the one to create):
/// 1. `LASTDB_HOME` environment variable (if set) — the new canonical override.
/// 2. `FOLDDB_HOME` environment variable (if set) — the legacy override, still
///    honored so existing launch-agents / scripts / `run.sh` keep working.
/// 3. `~/.lastdb` *if it already exists* — a node already migrated to the new
///    home keeps using it.
/// 4. `~/.folddb` *if it already exists* — an EXISTING (pre-rename) install,
///    including Tom's live `:9001` brain, keeps reading its data in place. No
///    move, no copy.
/// 5. `~/.lastdb` (created on demand) — a brand-new install with neither legacy
///    nor new home present gets the new default.
///
/// Net effect: NEW installs use `~/.lastdb`; EXISTING installs keep reading
/// `~/.folddb` with zero movement; an explicit `LASTDB_HOME`/`FOLDDB_HOME`
/// override wins over both. Migrating an existing home to `~/.lastdb` is an
/// explicit, opt-in operation a user performs deliberately — never automatic.
///
/// Env-var values are routed through [`expand_tilde`] so a literal `~/.folddb`
/// (e.g. from a launch-agent plist, Tauri env-injection, or a script that wrote
/// the value without shell expansion) doesn't reach `create_dir_all` / sled
/// with the literal `~` intact — that's the failure mode that left 596 MB of
/// live sled state under `/Users/example/~/.folddb/data/db` on Tom's machine
/// and a stray `./~/` directory in fbrain's cwd during the 2026-05-27 dogfood.
///
/// Returns an error if either: (a) an env override is set to a `~`-prefixed
/// value but `HOME` is unresolvable (per [`expand_tilde`] — no silent fall back
/// to a literal-`~` path), or (b) no env override is set AND the home directory
/// cannot be determined.
pub fn folddb_home() -> Result<PathBuf, String> {
    // 1. New canonical override.
    if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
        return expand_tilde(&home);
    }
    // 2. Legacy override (kept working).
    if let Ok(home) = std::env::var(FOLDDB_HOME_ENV) {
        return expand_tilde(&home);
    }
    let home = dirs::home_dir().ok_or_else(|| "Cannot determine home directory".to_string())?;
    Ok(resolve_default_node_home(&home, Path::is_dir))
}

/// Resolve the node's Unix-domain socket path.
///
/// Priority:
/// 1. `FOLDDB_SOCKET_PATH` — canonical explicit override.
/// 2. `FOLDDB_SOCK` — deprecated alias, still honored for old callers.
/// 3. `<node-home>/data/folddb.sock` — using [`folddb_home`]'s
///    `LASTDB_HOME` → `FOLDDB_HOME` → existing/default home order.
///
/// The returned path is not probed. Callers that need transport discovery
/// should check existence before selecting UDS over TCP.
pub fn node_socket_path_with_source() -> Result<(PathBuf, NodeSocketPathSource), String> {
    if let Ok(path) = std::env::var(FOLDDB_SOCKET_PATH_ENV) {
        if !path.is_empty() {
            return expand_tilde(&path).map(|p| (p, NodeSocketPathSource::CanonicalOverride));
        }
    }
    if let Ok(path) = std::env::var(FOLDDB_SOCK_ENV) {
        if !path.is_empty() {
            return expand_tilde(&path).map(|p| (p, NodeSocketPathSource::LegacyOverride));
        }
    }
    Ok((
        folddb_home()?.join("data").join(NODE_SOCKET_FILE_NAME),
        NodeSocketPathSource::NodeHomeDefault,
    ))
}

/// Pure resolution of the *default* node home (steps 3–5 of [`folddb_home`])
/// from an explicit `$HOME` and an injectable directory-existence predicate.
///
/// Factored out so the resolution ORDER can be unit-tested deterministically
/// WITHOUT mutating the process-global `HOME` env var (which would race every
/// other test that calls `folddb_home()` in parallel). Env-override handling
/// (steps 1–2) stays in [`folddb_home`] since it reads process env directly.
///
/// Order: an existing `~/.lastdb` wins (already-migrated); else an existing
/// `~/.folddb` is read in place (existing install — zero movement); else a new
/// `~/.lastdb` (brand-new install). `dir_exists` is the "is this an existing
/// directory?" test (`Path::is_dir` in production).
fn resolve_default_node_home(home: &Path, dir_exists: impl Fn(&Path) -> bool) -> PathBuf {
    let lastdb = home.join(LASTDB_DIR);
    // 3. An already-migrated new home wins.
    if dir_exists(&lastdb) {
        return lastdb;
    }
    // 4. An existing legacy home is read in place (zero movement) — this is the
    //    path Tom's live :9001 brain takes.
    let folddb = home.join(FOLDDB_DIR);
    if dir_exists(&folddb) {
        return folddb;
    }
    // 5. Brand-new install: default to the new home (created on demand by the
    //    caller's create_dir_all).
    lastdb
}

/// Expand a leading `~` or `~/` to the current user's home directory.
///
/// This is the single shared tilde-expansion helper for the node — callers
/// must never roll their own. Behavior:
///
/// - `~` alone or a `~/`-prefixed path → the remainder joined onto `$HOME`.
/// - Home directory unresolvable (`HOME` unset) → `Err`. Crucially this
///   NEVER returns a path that still contains a literal `~`: the older
///   inline implementations fell back to `PathBuf::from(raw)` here, which
///   made callers create directories like `<cwd>/~/.folddb/...` (a stray
///   `./~/.folddb/` was observed in the repo root during the 2026-05-21
///   dogfood). Treat the unresolvable-home case as the hard error it is.
/// - Any other input → returned unchanged.
pub fn expand_tilde(raw: &str) -> Result<PathBuf, String> {
    if raw == "~" || raw.starts_with("~/") {
        match std::env::var("HOME") {
            Ok(home) => {
                let rest = raw.strip_prefix("~/").unwrap_or("");
                Ok(PathBuf::from(home).join(rest))
            }
            Err(_) => Err(format!(
                "Cannot expand ~ in path \"{raw}\": HOME environment variable not set"
            )),
        }
    } else {
        Ok(PathBuf::from(raw))
    }
}

/// Tilde-expand a `PathBuf`/`Path` input.
///
/// Companion to [`expand_tilde`] for callers that already hold a path
/// (clap-parsed CLI args, serde-deserialized `PathBuf` fields). Without
/// this, `serde::Deserialize` on `PathBuf` accepts the string verbatim,
/// so a `node_config.json` with `"database.path": "~/.folddb/data"`
/// reaches the sled opener as a literal `~/.folddb/data` and creates
/// `<cwd>/~/.folddb/data` — the exact bug the 2026-05-21 dogfood
/// comment in [`expand_tilde`] warned about.
///
/// Behavior: if the path's string form is `~` or starts with `~/`,
/// route through [`expand_tilde`] (which propagates the
/// HOME-unresolvable error rather than silently returning a literal-`~`
/// `PathBuf`). Any other input is returned unchanged. Non-UTF-8 paths
/// pass through untouched — `expand_tilde`'s contract is string-based,
/// and non-UTF-8 cannot match the `~`/`~/` prefix anyway.
pub fn expand_tilde_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let path = path.as_ref();
    if let Some(s) = path.to_str() {
        if s == "~" || s.starts_with("~/") {
            return expand_tilde(s);
        }
    }
    Ok(path.to_path_buf())
}

/// Assert a resolved path does NOT still contain a literal `~`
/// component anywhere. Last-line-of-defense guard against future
/// regressions: every path that flows into a sled opener or
/// `create_dir_all` call should have been through [`expand_tilde`] /
/// [`expand_tilde_path`] by the time it reaches the storage layer, and
/// a leftover `~` means a code path bypassed that helper (the same
/// failure mode that left 596 MB of live sled state under
/// `/Users/example/~/.folddb/data/db` on Tom's machine).
///
/// Cheap to call at boot — single component walk over the resolved
/// path. Returns `Err` with an actionable diagnostic message; the
/// caller decides whether to panic (debug) or log-and-bail (release).
pub fn assert_no_literal_tilde(path: &Path) -> Result<(), String> {
    for component in path.components() {
        if let std::path::Component::Normal(os) = component {
            if os == "~" {
                return Err(format!(
                    "resolved path {path:?} still contains a literal `~` component — \
                     a code path bypassed expand_tilde / expand_tilde_path. \
                     This would create directories under `<cwd>/~/...` or \
                     `$HOME/~/...` and silently shard storage from the \
                     user's real data."
                ));
            }
        }
    }
    Ok(())
}
