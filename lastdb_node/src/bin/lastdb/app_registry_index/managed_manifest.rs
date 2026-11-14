//! Install instructions, step execution, atomic command links, and git source verification.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{str_err, ListEntry};

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
    if p.as_os_str().is_empty()
        || p.is_absolute()
        || !p.components().all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(format!(
            "install path must stay inside the source tree: {path}"
        ));
    }
    Ok(p.to_path_buf())
}

impl InstallPlan {
    pub(super) fn load(source: &Path, app: &str) -> Result<Self, String> {
        let raw = fs::read(source.join("lastdb-app.json")).map_err(str_err)?;
        let val: serde_json::Value = serde_json::from_slice(&raw).map_err(str_err)?;
        if val["app_id"].as_str() != Some(app) {
            return Err("source manifest app_id differs from signed listing".into());
        }
        let mut plan: Self = serde_json::from_value(val["install"].clone())
            .map_err(|e| format!("app needs an install manifest: {e}"))?;
        let uses = val
            .get("uses")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        plan.uses = serde_json::from_value(uses)
            .map_err(|_| "manifest uses must be an array of strings")?;
        if plan.probe.is_empty() || plan.links.is_empty() {
            return Err("install needs links and at least one probe".into());
        }
        let mut commands = HashSet::new();
        for l in &plan.links {
            relative(&l.source)?;
            let ok_chars = l
                .command
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
            if l.command.is_empty()
                || l.command.starts_with('-')
                || !ok_chars
                || !commands.insert(&l.command)
            {
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
        if !plan.requires.iter().all(|a| super::valid_app_id(a)) {
            return Err("invalid required app".into());
        }
        Ok(plan)
    }

    pub(super) fn consent_hash(&self) -> Result<String, String> {
        let val = serde_json::to_value(self).map_err(str_err)?;
        let bytes = app_identity_crypto::canonicalize(&val).map_err(str_err)?;
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
    let sock = lastdb_node::lastdb_uds::socket_path(&data);
    for step in list {
        let mut cmd = ProcessCommand::new(&step.argv[0]);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        cmd.args(&step.argv[1..]).current_dir(source).env_clear();
        cmd.stdin(Stdio::null())
            .stdout(Stdio::from(std::io::stderr()))
            .stderr(Stdio::inherit());
        cmd.envs(
            pass.split_whitespace()
                .filter_map(|k| Some((k, std::env::var_os(k)?))),
        );
        cmd.env("LASTDB_HOME", home).env("LASTDB_DATA_DIR", &data);
        cmd.env("LASTDB_SOCKET", &sock).env("LASTDB_APP_ID", app);
        cmd.env("LASTDB_APP_SOURCE_DIR", source)
            .env("LAST_STACK_SETUP_PRESERVE_SERVICES", "1");
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("cannot run {}: {e}", step.argv[0]))?;
        let start = Instant::now();
        loop {
            if let Some(st) = child.try_wait().map_err(str_err)? {
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
    let home = std::env::var_os("HOME").ok_or("HOME is required")?;
    Ok(PathBuf::from(home).join(".local/bin"))
}

pub(super) fn validate_sources(source: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source = fs::canonicalize(source).map_err(str_err)?;
    for link in &plan.links {
        if matches!(
            link.command.as_str(),
            "lastdb" | "lastdbd" | "folddb" | "host-track"
        ) {
            return Err("app links cannot replace the primary program pair or host-track".into());
        }
        let path = fs::canonicalize(source.join(relative(&link.source)?)).map_err(str_err)?;
        if !path.starts_with(&source) || !path.is_file() {
            return Err("app command escapes source tree".into());
        }
    }
    Ok(())
}

pub(super) fn flip(link: &Path, target: &Path) -> Result<(), String> {
    let tmp = link.with_extension(format!("new-{}", std::process::id()));
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, &tmp).map_err(str_err)?;
    #[cfg(not(unix))]
    return Err("managed app links require a Unix host".into());
    fs::rename(&tmp, link).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        e.to_string()
    })
}

pub(super) fn install(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let (src, bin) = (root.join("current/source"), bin_root()?);
    validate_sources(&src, plan)?;
    fs::create_dir_all(&bin).map_err(str_err)?;
    for l in &plan.links {
        flip(&bin_link_path(&bin, &l.command)?, &src.join(&l.source))?;
    }
    Ok(())
}

pub(super) fn verify(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    let (src, bin) = (root.join("current/source"), bin_root()?);
    validate_sources(&src, plan)?;
    for l in &plan.links {
        if fs::read_link(bin.join(&l.command)).map_err(str_err)? != src.join(&l.source) {
            return Err(format!("command link drift: {}", l.command));
        }
    }
    Ok(())
}

pub(super) fn snapshot(plan: &InstallPlan) -> Result<Vec<(PathBuf, Option<PathBuf>)>, String> {
    let bin = bin_root()?;
    let mut out = Vec::with_capacity(plan.links.len());
    for l in &plan.links {
        let p = bin_link_path(&bin, &l.command)?;
        out.push((p.clone(), fs::read_link(&p).ok()));
    }
    Ok(out)
}

pub(super) fn restore(snapshot: &[(PathBuf, Option<PathBuf>)]) -> Result<(), String> {
    for (path, target) in snapshot {
        if let Some(t) = target {
            flip(path, t)?;
        } else if path.symlink_metadata().is_ok() {
            fs::remove_file(path).map_err(str_err)?;
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
        .map_err(str_err)?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("git {} failed", args[0]))
    }
}

pub(super) fn clone_source(checkout: &Path, entry: &ListEntry) -> Result<(), String> {
    if entry.sha.len() != 40 || !entry.sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            entry.sha
        ));
    }
    let mut c = ProcessCommand::new("git");
    c.args(["clone", "--quiet", "--no-checkout", &entry.source])
        .arg(checkout);
    let st = c
        .status()
        .map_err(|e| format!("failed to run git clone: {e}"))?;
    if !st.success() {
        return Err(format!(
            "git clone failed for app '{}' from {}",
            entry.app_id, entry.source
        ));
    }
    if git_ok(checkout, &["checkout", "--quiet", "--detach", &entry.sha]).is_err() {
        return Err(format!(
            "git checkout {} failed for app '{}': the proved commit is not in {}",
            entry.sha, entry.app_id, entry.source
        ));
    }
    let head = git_ok(checkout, &["rev-parse", "HEAD"])
        .map_err(|e| format!("failed to run git rev-parse: {e}"))?;
    if head != entry.sha {
        return Err(format!(
            "checkout HEAD is {head} but the proved commit is {}",
            entry.sha
        ));
    }
    Ok(())
}

pub(super) fn verify_git_source(source: &Path, expected_sha: &str) -> Result<(), String> {
    if git_ok(source, &["rev-parse", "HEAD"]).ok().as_deref() != Some(expected_sha) {
        return Err("installed source commit drift".into());
    }
    git_ok(source, &["diff", "--quiet", "HEAD", "--"])
        .map(|_| ())
        .map_err(|_| "installed tracked source changed".into())
}
