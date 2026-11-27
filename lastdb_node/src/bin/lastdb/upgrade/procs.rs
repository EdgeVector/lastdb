//! Running helper programs with a time limit.

use std::io::Read as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub(super) struct Captured {
    pub(super) ok: bool,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: String,
}

impl Captured {
    pub(super) fn stdout_text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }
}

/// Run `program` with `args` and wait at most `limit`. A program that outlives the
/// limit is a helper of this command (never the node), so it is stopped.
pub(super) fn run(program: &Path, args: &[&str], limit: Duration) -> Result<Captured, String> {
    let mut child = Command::new(program)
        .args(args)
        .env("RUST_LOG", "off")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("cannot run {}: {e}", program.display()))?;
    let drain = |pipe: Option<Box<dyn std::io::Read + Send>>| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_end(&mut bytes);
            }
            bytes
        })
    };
    let out = drain(
        child
            .stdout
            .take()
            .map(|p| Box::new(p) as Box<dyn std::io::Read + Send>),
    );
    let err = drain(
        child
            .stderr
            .take()
            .map(|p| Box::new(p) as Box<dyn std::io::Read + Send>),
    );
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if start.elapsed() >= limit => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "{} did not finish within {} seconds",
                    program.display(),
                    limit.as_secs()
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => return Err(format!("cannot wait for {}: {e}", program.display())),
        }
    };
    let stdout = out.join().unwrap_or_default();
    let stderr = String::from_utf8_lossy(&err.join().unwrap_or_default()).into_owned();
    Ok(Captured {
        ok: status.success(),
        stdout,
        stderr,
    })
}

/// The first line of a program's `--version`, its last word.
pub(super) fn version_word(program: &Path) -> Result<String, String> {
    let out = run(program, &["--version"], Duration::from_secs(15))?;
    if !out.ok {
        return Err(format!("{} --version failed", program.display()));
    }
    out.stdout_text()
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().last())
        .map(str::to_owned)
        .ok_or_else(|| format!("{} --version printed nothing", program.display()))
}

/// Whether the process `pid` still exists.
pub(super) fn alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Resident memory of `pid` in megabytes, or `None` when `ps` cannot tell.
pub(super) fn rss_mb(pid: u32) -> Option<u64> {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let kb: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kb / 1024)
}
