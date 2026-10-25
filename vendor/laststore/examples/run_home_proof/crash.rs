//! kill -9 recovery: a child process commits and merges until the parent
//! sends SIGKILL. The reopened home must hold every acked commit and at most
//! one more, and every key must match the model of exactly that many commits.

use crate::gen::{apply, batch, Model};
use crate::{check_all, run_files, Result};
use laststore::disk_run::home::RunHome;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

const CRASH_OPS: usize = 500;
const MAX_CHILD_BATCHES: u64 = 100_000;

/// The child side. The home key arrives on stdin, never in arguments.
pub fn child(dir: &Path) -> Result<()> {
    let mut key = [0u8; 32];
    std::io::stdin().read_exact(&mut key)?;
    let home = RunHome::open(dir, &key)?;
    let start = home.status().frontier + 1;
    let mut out = std::io::stdout().lock();
    for n in start..start + MAX_CHILD_BATCHES {
        let commit = home.commit(batch(n, CRASH_OPS))?;
        writeln!(out, "acked {}", commit.frontier)?;
        out.flush()?;
        while home.merge_step()?.is_some() {}
    }
    Ok(())
}

fn one_round(dir: &Path, key: &[u8; 32], kill_after: u64) -> Result<u64> {
    let mut child = Command::new(std::env::current_exe()?)
        .arg("child")
        .arg(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    child.stdin.take().ok_or("child stdin")?.write_all(key)?;
    let mut lines = BufReader::new(child.stdout.take().ok_or("child stdout")?).lines();
    let mut acked = 0u64;
    let mut seen = 0u64;
    while seen < kill_after {
        let line = lines.next().ok_or("child stopped before the kill")??;
        acked = parse_ack(&line)?;
        seen += 1;
    }
    // std's kill sends SIGKILL on Unix: no destructor, no flush, no unwind.
    child.kill()?;
    child.wait()?;
    // Lines already in the pipe are commits that returned before the kill.
    for line in lines {
        acked = parse_ack(&line?)?;
    }
    Ok(acked)
}

fn parse_ack(line: &str) -> Result<u64> {
    Ok(line
        .strip_prefix("acked ")
        .ok_or("unexpected child output")?
        .parse()?)
}

/// The parent side: three kills at different points, then full checks.
pub fn kill_nine(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let dir = scratch.join("crash");
    drop(RunHome::create(&dir, key)?);
    let mut report = Vec::new();
    for kill_after in [15, 40, 90] {
        let acked = one_round(&dir, key, kill_after)?;
        let home = RunHome::open(&dir, key)?;
        let frontier = home.status().frontier;
        if frontier < acked || frontier > acked + 1 {
            return Err(format!("acked {acked} commits but reopened at {frontier}").into());
        }
        let (named, leftovers) = run_files(&dir)?;
        let in_current = home.status().runs();
        if named != in_current || leftovers != 0 {
            return Err(format!(
                "after reopen: {named} run files, {in_current} in CURRENT, {leftovers} temporary"
            )
            .into());
        }
        let mut model = Model::new();
        for n in 1..=frontier {
            apply(&mut model, &batch(n, CRASH_OPS));
        }
        let checked = check_all(&home, &model)?;
        report.push(format!(
            "kill at ack {kill_after}: acked {acked}, reopened at {frontier}, {checked} keys equal"
        ));
    }
    Ok(report.join("; "))
}
