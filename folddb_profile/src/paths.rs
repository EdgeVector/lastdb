//! LastDB node-home path resolution + tilde expansion.
//!
//! A single resolver controls where all instance-specific state lives:
//! `LASTDB_HOME` when it is set, else `~/.lastdb`. See [`folddb_home`].
//!
//! This family used to live in `fold_db_node::utils::paths`; the node-home
//! resolution + tilde-expansion helpers moved here so the profile loader (and
//! the `folddb dev` dev node) can find the profile file without depending on
//! fold_db_node. `fold_db_node::utils::paths` re-exports these and keeps its
//! node-specific helpers (`observability_log_path`, `onboarding_marker_path`)
//! local.

use std::path::{Path, PathBuf};

/// Directory name of the default node home.
pub const LASTDB_DIR: &str = ".lastdb";
/// Node-home override.
pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

/// Resolve the node-home directory: the `LASTDB_HOME` environment variable
/// when it is set, else `~/.lastdb` (created on demand by the caller). This
/// resolver never moves or copies data.
///
/// The env value goes through [`expand_tilde`] so a literal `~/.lastdb`
/// (e.g. from a launch-agent plist or a script that wrote the value without
/// shell expansion) doesn't reach `create_dir_all` with the literal `~`
/// intact, which would create a stray `<cwd>/~/` directory.
///
/// Returns an error if either: (a) `LASTDB_HOME` is set to a `~`-prefixed
/// value but `HOME` is unresolvable (per [`expand_tilde`] — no silent fall back
/// to a literal-`~` path), or (b) `LASTDB_HOME` is not set AND the home
/// directory cannot be determined.
pub fn folddb_home() -> Result<PathBuf, String> {
    if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
        return expand_tilde(&home);
    }
    let home = dirs::home_dir().ok_or_else(|| "Cannot determine home directory".to_string())?;
    Ok(home.join(LASTDB_DIR))
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
