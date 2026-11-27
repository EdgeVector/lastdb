//! Bake the Sentry release SHA and optional user-device DSN at compile time.

use std::process::Command;

fn main() {
    for key in ["GITHUB_SHA", "OBS_SENTRY_RELEASE", "LASTDB_SENTRY_DSN"] {
        println!("cargo:rerun-if-env-changed={key}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    if let Some(git_dir) = git_path(&["rev-parse", "--git-dir"]) {
        let head = git_dir.join("HEAD");
        if head.exists() {
            println!("cargo:rerun-if-changed={}", head.display());
        }
    }
    println!("cargo:rustc-env=OBS_RELEASE_SHA={}", resolve_release_sha());
    if let Ok(dsn) = std::env::var("LASTDB_SENTRY_DSN") {
        let dsn = dsn.trim();
        if !dsn.is_empty() {
            println!("cargo:rustc-env=LASTDB_SENTRY_DSN={dsn}");
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

fn git_path(args: &[&str]) -> Option<std::path::PathBuf> {
    let p = std::path::PathBuf::from(git_output(args)?);
    if p.is_absolute() {
        Some(p)
    } else {
        Some(std::env::current_dir().ok()?.join(p))
    }
}
