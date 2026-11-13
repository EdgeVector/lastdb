//! Install instructions, step execution, atomic command links, and git source verification.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ListEntry;

const fn default_timeout() -> u64 {
    30
}
fn default_exits() -> Vec<i32> {
    vec![0]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstallStep {
    pub argv: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
    #[serde(default = "default_exits")]
    pub accept_exit: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstallLink {
    pub source: String,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstallPlan {
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
    pub(super) fn load(source: &Path, app: &str) -> Result<Self, String> {
        let val: serde_json::Value = serde_json::from_slice(
            &fs::read(source.join("lastdb-app.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if val["app_id"].as_str() != Some(app) {
            return Err("source manifest app_id differs from signed listing".into());
        }
        let mut plan: Self = serde_json::from_value(val["install"].clone())
            .map_err(|e| format!("app needs an install manifest: {e}"))?;
        plan.uses = serde_json::from_value(
            val.get("uses")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        )
        .map_err(|_| "manifest uses must be an array of strings")?;
        if plan.probe.is_empty() || plan.links.is_empty() {
            return Err("install needs links and at least one probe".into());
        }
        let mut commands = HashSet::new();
        for l in &plan.links {
            relative(&l.source)?;
            let valid = !l.command.is_empty()
                && !l.command.starts_with('-')
                && l.command
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
            if !valid || !commands.insert(&l.command) {
                return Err("invalid or duplicate install command".into());
            }
        }
        for s in plan
            .prepare
            .iter()
            .chain(&plan.post_install)
            .chain(&plan.probe)
        {
            if s.argv.is_empty()
                || s.argv[0].is_empty()
                || !(1..=600).contains(&s.timeout_s)
                || s.accept_exit.is_empty()
            {
                return Err("invalid install step or timeout (1..600 seconds)".into());
            }
        }
        plan.requires
            .iter()
            .all(|a| super::valid_app_id(a))
            .then_some(plan)
            .ok_or_else(|| "invalid required app".into())
    }

    pub(super) fn consent_hash(&self) -> Result<String, String> {
        let bytes = app_identity_crypto::canonicalize(
            &serde_json::to_value(self).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

pub(super) fn steps(
    source: &Path,
    home: &Path,
    app: &str,
    list: &[InstallStep],
) -> Result<(), String> {
    let pass =
        "HOME PATH TMPDIR USER LOGNAME SSH_AUTH_SOCK LASTDB_APPS_ROOT LANG LAST_STACK_APPS_OFF";
    let data = home.join("data");
    for step in list {
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
            .stderr(Stdio::inherit())
            .envs(
                pass.split_whitespace()
                    .filter_map(|k| Some((k, std::env::var_os(k)?))),
            )
            .env("LASTDB_HOME", home)
            .env("LASTDB_DATA_DIR", &data)
            .env("LASTDB_SOCKET", crate::lastdb_uds::uds::socket_path(&data))
            .env("LASTDB_APP_ID", app)
            .env("LASTDB_APP_SOURCE_DIR", source)
            .env("LAST_STACK_SETUP_PRESERVE_SERVICES", "1");
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("cannot run {}: {e}", step.argv[0]))?;
        let start = Instant::now();
        loop {
            if let Some(st) = child.try_wait().map_err(|e| e.to_string())? {
                if st.code().is_some_and(|c| step.accept_exit.contains(&c)) {
                    break;
                }
                return Err(format!("install step {} exited {st}", step.argv[0]));
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
    Ok(())
}

fn bin_link_path(bin: &Path, command: &str) -> Result<PathBuf, String> {
    let p = bin.join(command);
    if p.symlink_metadata()
        .is_ok_and(|m| !m.file_type().is_symlink())
    {
        return Err(format!(
            "refusing to replace a regular file: {}",
            p.display()
        ));
    }
    Ok(p)
}

fn bin_root() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(|p| PathBuf::from(p).join(".local/bin"))
        .ok_or_else(|| "HOME is required".into())
}

pub(super) fn validate_sources(source: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source = fs::canonicalize(source).map_err(|e| e.to_string())?;
    for link in &plan.links {
        if matches!(
            link.command.as_str(),
            "lastdb" | "lastdbd" | "folddb" | "host-track"
        ) {
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

pub(super) fn flip(link: &Path, target: &Path) -> Result<(), String> {
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

fn validated_bins(root: &Path, plan: &InstallPlan) -> Result<(PathBuf, PathBuf), String> {
    let (src, bin) = (root.join("current/source"), bin_root()?);
    validate_sources(&src, plan)?;
    Ok((src, bin))
}

pub(super) fn install(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let (src, bin) = validated_bins(root, plan)?;
    fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
    plan.links
        .iter()
        .try_for_each(|l| flip(&bin_link_path(&bin, &l.command)?, &src.join(&l.source)))
}

pub(super) fn verify(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let (src, bin) = validated_bins(root, plan)?;
    plan.links.iter().try_for_each(|l| {
        (fs::read_link(bin.join(&l.command)).map_err(|e| e.to_string())? == src.join(&l.source))
            .then_some(())
            .ok_or_else(|| format!("command link drift: {}", l.command))
    })
}

pub(super) fn snapshot(plan: &InstallPlan) -> Result<Vec<(PathBuf, Option<PathBuf>)>, String> {
    let bin = bin_root()?;
    plan.links
        .iter()
        .map(|l| {
            let p = bin_link_path(&bin, &l.command)?;
            Ok((p.clone(), fs::read_link(&p).ok()))
        })
        .collect()
}

pub(super) fn restore(snapshot: &[(PathBuf, Option<PathBuf>)]) -> Result<(), String> {
    for (path, target) in snapshot {
        if let Some(target) = target {
            flip(path, target)?;
        } else if path.symlink_metadata().is_ok() {
            fs::remove_file(path).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn git_ok(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = ProcessCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .ok_or_else(|| format!("git {} failed", args[0]))
}

pub(super) fn clone_source(checkout: &Path, entry: &ListEntry) -> Result<(), String> {
    if entry.sha.len() != 40 || !entry.sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            entry.sha
        ));
    }
    let out = ProcessCommand::new("git")
        .args(["clone", "--quiet", "--no-checkout", &entry.source])
        .arg(checkout)
        .status()
        .map_err(|e| format!("failed to run git clone: {e}"))?;
    if !out.success() {
        return Err(format!(
            "git clone failed for app '{}' from {}",
            entry.app_id, entry.source
        ));
    }
    git_ok(checkout, &["checkout", "--quiet", "--detach", &entry.sha]).map_err(|_| {
        format!(
            "git checkout {} failed for app '{}': the proved commit is not in {}",
            entry.sha, entry.app_id, entry.source
        )
    })?;
    let head = git_ok(checkout, &["rev-parse", "HEAD"])
        .map_err(|e| format!("failed to run git rev-parse: {e}"))?;
    (head == entry.sha).then_some(()).ok_or_else(|| {
        format!(
            "checkout HEAD is {head} but the proved commit is {}",
            entry.sha
        )
    })
}

pub(super) fn verify_git_source(source: &Path, expected_sha: &str) -> Result<(), String> {
    if git_ok(source, &["rev-parse", "HEAD"]).ok().as_deref() != Some(expected_sha) {
        return Err("installed source commit drift".into());
    }
    git_ok(source, &["diff", "--quiet", "HEAD", "--"])
        .map(|_| ())
        .map_err(|_| "installed tracked source changed".into())
}
