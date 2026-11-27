//! Bake the Sentry release SHA and user-device DSN at compile time.

use std::path::PathBuf;
use std::process::Command;

/// Public DSN for the `lastdb-mini` Sentry project (not a secret).
const DEFAULT_USER_DEVICE_DSN: &str =
    "https://2de2e2512878eedeef5359696ac37e70@o4511522196291584.ingest.us.sentry.io/4511737993101312";

fn main() {
    for key in ["GITHUB_SHA", "OBS_SENTRY_RELEASE", "LASTDB_SENTRY_DSN"] {
        println!("cargo:rerun-if-env-changed={key}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    track_git_refs();
    println!("cargo:rustc-env=OBS_RELEASE_SHA={}", resolve_release_sha());
    let dsn = std::env::var("LASTDB_SENTRY_DSN")
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_USER_DEVICE_DSN.to_owned());
    println!("cargo:rustc-env=LASTDB_SENTRY_DSN={dsn}");
}

fn track_git_refs() {
    let Some(git_dir) = git_path(&["rev-parse", "--git-dir"]) else {
        return;
    };
    let common = git_path(&["rev-parse", "--git-common-dir"]).unwrap_or_else(|| git_dir.clone());
    let sym = git_output(&["symbolic-ref", "-q", "HEAD"]).map(|r| common.join(r));
    for p in [
        Some(git_dir.join("HEAD")),
        sym,
        Some(common.join("packed-refs")),
    ]
    .into_iter()
    .flatten()
    {
        if p.exists() {
            println!("cargo:rerun-if-changed={}", p.display());
        }
    }
}

fn resolve_release_sha() -> String {
    if let Ok(v) = std::env::var("OBS_SENTRY_RELEASE") {
        let v = v.trim();
        if !v.is_empty() {
            return v.to_owned();
        }
    }
    if let Ok(v) = std::env::var("GITHUB_SHA") {
        let v = v.trim();
        if !v.is_empty() {
            return v.to_owned();
        }
    }
    if let Some(sha) = git_output(&["rev-parse", "HEAD"]) {
        return sha;
    }
    panic!(
        "observability: compile-time release SHA required \
         (set GITHUB_SHA, OBS_SENTRY_RELEASE, or build inside a git checkout)"
    );
}

fn git_output(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status.success().then_some(())?;
    let s = String::from_utf8(out.stdout).ok()?;
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_owned())
}

fn git_path(args: &[&str]) -> Option<PathBuf> {
    let p = PathBuf::from(git_output(args)?);
    if p.is_absolute() {
        Some(p)
    } else {
        Some(std::env::current_dir().ok()?.join(p))
    }
}
