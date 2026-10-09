//! Stamp `FOLDDB_BUILD_VERSION` into the `lastdbd` binary at compile time
//! (tag → git describe → manifest version), so the release gate's
//! `--version`-matches-tag check holds for this binary.
//!
//! The stamp is not decoration: `--version`, the session ledger, and crash
//! attribution all read it to name the source a running daemon was built from.
//! It must therefore track git state rather than whatever cargo happened to
//! cache — see [`emit_git_rerun_triggers`].
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=GITHUB_REF_NAME");
    println!("cargo:rerun-if-env-changed=FOLDDB_BUILD_VERSION_OVERRIDE");
    println!("cargo:rerun-if-changed=build.rs");
    emit_git_rerun_triggers();

    let version = resolve_version();
    println!("cargo:rustc-env=FOLDDB_BUILD_VERSION={version}");
}

/// Declare the git state this script's output depends on.
///
/// [`git_describe`] changes whenever HEAD moves (commit, checkout, reset) or a
/// tag lands, but cargo cannot see that on its own: with no declared
/// dependency it caches the build-script output and keeps re-stamping a stale
/// version across commits until `build.rs` itself changes.
///
/// That is invisible in CI — release builds start from a fresh clone, so the
/// cache is always cold — and bites exactly the incremental local builds the
/// `lastdb-safe-upgrade` candidate path uses, where the stamp is what tells an
/// operator whether a cutover took, what the session ledger records, and what
/// crash attribution blames. A binary built from one commit but stamped with
/// another sends every one of those readers to the wrong source.
///
/// Only paths that already exist are emitted: cargo re-runs a build script
/// whenever a declared `rerun-if-changed` path is missing, which would defeat
/// caching entirely for anyone building from a git-less source tarball.
fn emit_git_rerun_triggers() {
    let Some(git_dir) = git_path(&["rev-parse", "--git-dir"]) else {
        return;
    };
    // Inside a `git worktree` checkout — how all EdgeVector dev happens, where
    // `.git` is a file rather than a directory — HEAD lives in the per-worktree
    // git dir while refs live in the shared common dir. Outside one the two are
    // the same path.
    let common_dir =
        git_path(&["rev-parse", "--git-common-dir"]).unwrap_or_else(|| git_dir.clone());

    rerun_if_exists(&git_dir.join("HEAD"));

    // A commit or `reset --hard` rewrites the current branch's loose ref; a
    // fetch or `git pack-refs` can fold it into packed-refs instead. Watch both
    // so neither route silently keeps the cached stamp.
    if let Some(reference) = git_output(&["symbolic-ref", "-q", "HEAD"]) {
        rerun_if_exists(&common_dir.join(reference));
    }
    rerun_if_exists(&common_dir.join("packed-refs"));
}

/// Emit `rerun-if-changed` for `path` only when it exists (see
/// [`emit_git_rerun_triggers`] for why a missing path must stay unemitted).
fn rerun_if_exists(path: &Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

/// Run `git args...` and return trimmed stdout, or `None` when git is absent,
/// this is not a repository, or the output is empty.
fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8(output.stdout).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

/// Resolve a git-reported path to an absolute one. `git rev-parse` answers
/// relative to the invoking directory (plain `.git` when run at a repo root),
/// so a bare relative answer is joined onto the build script's cwd rather than
/// used as-is.
fn git_path(args: &[&str]) -> Option<PathBuf> {
    let raw = PathBuf::from(git_output(args)?);
    if raw.is_absolute() {
        return Some(raw);
    }
    Some(std::env::current_dir().ok()?.join(raw))
}

fn resolve_version() -> String {
    if let Ok(override_val) = std::env::var("FOLDDB_BUILD_VERSION_OVERRIDE") {
        let trimmed = override_val.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Ok(ref_name) = std::env::var("GITHUB_REF_NAME") {
        if let Some(stripped) = strip_tag_prefix(&ref_name) {
            return stripped;
        }
    }

    if let Some(described) = git_describe() {
        return described;
    }

    env!("CARGO_PKG_VERSION").to_string()
}

/// Strip the leading `v` from `v0.3.1`-style tags; `None` for non-tag refs.
fn strip_tag_prefix(ref_name: &str) -> Option<String> {
    let trimmed = ref_name.trim();
    let rest = trimmed.strip_prefix('v')?;
    let first = rest.chars().next()?;
    if first.is_ascii_digit() {
        Some(rest.to_string())
    } else {
        None
    }
}

fn git_describe() -> Option<String> {
    let described = git_output(&["describe", "--tags", "--always", "--dirty"])?;
    Some(strip_tag_prefix(&described).unwrap_or(described))
}
