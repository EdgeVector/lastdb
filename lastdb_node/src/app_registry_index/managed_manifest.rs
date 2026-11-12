//! Install instructions, step execution, atomic command links, and publisher transitions.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ListEntry;

fn default_timeout() -> u64 {
    30
}

fn default_exits() -> Vec<i32> {
    vec![0]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallStep {
    pub argv: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
    #[serde(default = "default_exits")]
    pub accept_exit: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallLink {
    pub source: String,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallPlan {
    #[serde(default, skip_deserializing)]
    pub uses: Vec<String>,
    #[serde(default)]
    pub prepare: Vec<InstallStep>,
    #[serde(default)]
    pub post_install: Vec<InstallStep>,
    pub probe: Vec<InstallStep>,
    pub links: Vec<InstallLink>,
    #[serde(default)]
    pub requires: Vec<String>,
}

fn relative(path: &str) -> Result<PathBuf, String> {
    let p = Path::new(path);
    (!p.as_os_str().is_empty()
        && !p.is_absolute()
        && p.components().all(|c| matches!(c, Component::Normal(_))))
    .then(|| p.to_path_buf())
    .ok_or_else(|| format!("install path must stay inside the source tree: {path}"))
}

impl InstallPlan {
    pub fn load(source: &Path, app: &str) -> Result<Self, String> {
        let value: serde_json::Value = serde_json::from_slice(
            &fs::read(source.join("lastdb-app.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if value["app_id"].as_str() != Some(app) {
            return Err("source manifest app_id differs from signed listing".into());
        }
        let mut plan: Self = serde_json::from_value(value["install"].clone())
            .map_err(|e| format!("app needs an install manifest: {e}"))?;
        plan.uses = serde_json::from_value(
            value
                .get("uses")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        )
        .map_err(|_| "manifest uses must be an array of strings")?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&self) -> Result<(), String> {
        if self.probe.is_empty() || self.links.is_empty() {
            return Err("install needs links and at least one probe".into());
        }
        let mut commands = HashSet::new();
        for link in &self.links {
            relative(&link.source)?;
            let ok = !link.command.is_empty()
                && !link.command.starts_with('-')
                && link
                    .command
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                && commands.insert(&link.command);
            if !ok {
                return Err("invalid or duplicate install command".into());
            }
        }
        for step in self
            .prepare
            .iter()
            .chain(&self.post_install)
            .chain(&self.probe)
        {
            if step.argv.is_empty()
                || step.argv[0].is_empty()
                || !(1..=600).contains(&step.timeout_s)
                || step.accept_exit.is_empty()
            {
                return Err("invalid install step or timeout (1..600 seconds)".into());
            }
        }
        self.requires
            .iter()
            .all(|app| super::valid_app_id(app))
            .then_some(())
            .ok_or_else(|| "invalid required app".into())
    }

    pub fn consent_hash(&self) -> Result<String, String> {
        let value = serde_json::to_value(self).map_err(|e| e.to_string())?;
        let bytes = app_identity_crypto::canonicalize(&value).map_err(|e| e.to_string())?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

pub(crate) fn steps(
    source: &Path,
    home: &Path,
    app: &str,
    list: &[InstallStep],
) -> Result<(), String> {
    list.iter()
        .try_for_each(|step| run_step(source, home, app, step))
}

fn run_step(source: &Path, home: &Path, app: &str, step: &InstallStep) -> Result<(), String> {
    let mut cmd = ProcessCommand::new(&step.argv[0]);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd.args(&step.argv[1..])
        .current_dir(source)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::from(std::io::stderr()))
        .stderr(Stdio::inherit());
    for key in [
        "HOME",
        "PATH",
        "TMPDIR",
        "USER",
        "LOGNAME",
        "SSH_AUTH_SOCK",
        "LASTDB_APPS_ROOT",
        "LANG",
        "LAST_STACK_APPS_OFF",
    ] {
        if let Some(v) = std::env::var_os(key) {
            cmd.env(key, v);
        }
    }
    cmd.env("LASTDB_HOME", home)
        .env("LASTDB_DATA_DIR", home.join("data"))
        .env(
            "LASTDB_SOCKET",
            crate::lastdb_uds::uds::socket_path(&home.join("data")),
        )
        .env("LASTDB_APP_ID", app)
        .env("LASTDB_APP_SOURCE_DIR", source)
        .env("LAST_STACK_SETUP_PRESERVE_SERVICES", "1");
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("cannot run {}: {e}", step.argv[0]))?;
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return if status.code().is_some_and(|c| step.accept_exit.contains(&c)) {
                Ok(())
            } else {
                Err(format!("install step {} exited {status}", step.argv[0]))
            };
        }
        if start.elapsed() >= Duration::from_secs(step.timeout_s) {
            #[cfg(unix)]
            let _ = ProcessCommand::new("/bin/kill")
                .args(["-KILL", &format!("-{}", child.id())])
                .output();
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "install step {} exceeded {} seconds",
                step.argv[0], step.timeout_s
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn bin_root() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(|p| PathBuf::from(p).join(".local/bin"))
        .ok_or_else(|| "HOME is required".into())
}

fn check_replaceable(path: &Path) -> Result<(), String> {
    (!path
        .symlink_metadata()
        .is_ok_and(|m| !m.file_type().is_symlink()))
    .then_some(())
    .ok_or_else(|| format!("refusing to replace a regular file: {}", path.display()))
}

pub(crate) fn validate_sources(source: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source = fs::canonicalize(source).map_err(|e| e.to_string())?;
    for link in &plan.links {
        if ["lastdb", "lastdbd", "folddb", "host-track"].contains(&link.command.as_str()) {
            return Err("app links cannot replace the primary program pair or host-track".into());
        }
        let path =
            fs::canonicalize(source.join(relative(&link.source)?)).map_err(|e| e.to_string())?;
        if !path.starts_with(&source) || !path.is_file() {
            return Err("app command escapes source tree".into());
        }
    }
    Ok(())
}

pub(crate) fn flip(link: &Path, target: &Path) -> Result<(), String> {
    let tmp = link.with_extension(format!("new-{}", std::process::id()));
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, &tmp).map_err(|e| e.to_string())?;
    #[cfg(not(unix))]
    return Err("managed app links require a Unix host".into());
    fs::rename(&tmp, link).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        e.to_string()
    })
}

pub(crate) fn install(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let src = root.join("current/source");
    validate_sources(&src, plan)?;
    let bin = bin_root()?;
    fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
    for link in &plan.links {
        let target = bin.join(&link.command);
        check_replaceable(&target)?;
        flip(&target, &src.join(&link.source))?;
    }
    Ok(())
}

pub(crate) fn verify(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let src = root.join("current/source");
    validate_sources(&src, plan)?;
    let bin = bin_root()?;
    for link in &plan.links {
        if fs::read_link(bin.join(&link.command)).map_err(|e| e.to_string())?
            != src.join(&link.source)
        {
            return Err(format!("command link drift: {}", link.command));
        }
    }
    Ok(())
}

pub(crate) fn snapshot(plan: &InstallPlan) -> Result<Vec<(PathBuf, Option<PathBuf>)>, String> {
    let bin = bin_root()?;
    plan.links
        .iter()
        .map(|link| {
            let path = bin.join(&link.command);
            check_replaceable(&path)?;
            Ok((path.clone(), fs::read_link(&path).ok()))
        })
        .collect()
}

pub(crate) fn restore(snapshot: &[(PathBuf, Option<PathBuf>)]) -> Result<(), String> {
    for (path, target) in snapshot {
        if let Some(target) = target {
            flip(path, target)?;
        } else if path.symlink_metadata().is_ok() {
            fs::remove_file(path).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

pub(crate) fn clone_source(checkout_path: &Path, entry: &ListEntry) -> Result<(), String> {
    if entry.sha.len() != 40 || !entry.sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            entry.sha
        ));
    }
    if !git_ok(
        None,
        &["clone", "--quiet", "--no-checkout", &entry.source],
        Some(checkout_path),
    )
    .map_err(|e| format!("failed to run git clone: {e}"))?
    {
        return Err(format!(
            "git clone failed for app '{}' from {}",
            entry.app_id, entry.source
        ));
    }
    if !git_ok(
        Some(checkout_path),
        &["checkout", "--quiet", "--detach", &entry.sha],
        None,
    )
    .map_err(|e| format!("failed to run git checkout: {e}"))?
    {
        return Err(format!(
            "git checkout {} failed for app '{}': the proved commit is not in {}",
            entry.sha, entry.app_id, entry.source
        ));
    }
    let head = git_head(checkout_path).map_err(|e| format!("failed to run git rev-parse: {e}"))?;
    (head == entry.sha).then_some(()).ok_or_else(|| {
        format!(
            "checkout HEAD is {head} but the proved commit is {}",
            entry.sha
        )
    })
}

pub(crate) fn verify_git_source(source: &Path, expected_sha: &str) -> Result<(), String> {
    if git_head(source).ok().as_deref() != Some(expected_sha) {
        return Err("installed source commit drift".into());
    }
    git_ok(Some(source), &["diff", "--quiet", "HEAD", "--"], None)?
        .then_some(())
        .ok_or_else(|| "installed tracked source changed".into())
}

fn git_ok(cwd: Option<&Path>, args: &[&str], extra: Option<&Path>) -> Result<bool, String> {
    let mut cmd = ProcessCommand::new("git");
    if let Some(dir) = cwd {
        cmd.arg("-C").arg(dir);
    }
    cmd.args(args);
    if let Some(path) = extra {
        cmd.arg(path);
    }
    cmd.status().map(|s| s.success()).map_err(|e| e.to_string())
}

fn git_head(source: &Path) -> Result<String, String> {
    let out = ProcessCommand::new("git")
        .arg("-C")
        .arg(source)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| e.to_string())?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .ok_or_else(|| "git rev-parse HEAD failed".into())
}
