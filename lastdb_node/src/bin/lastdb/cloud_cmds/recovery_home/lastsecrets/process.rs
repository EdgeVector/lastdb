//! Bounded owned-child stdin and private readback. Never return child output errors.

use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

use super::LastSecrets;

const DEADLINE: Duration = Duration::from_secs(60);
const READBACK_LIMIT: usize = 4097;

pub(super) fn run(tool: &LastSecrets, key: Option<&[u8]>) -> Result<Vec<u8>, &'static str> {
    let mut command = tool.command(key.is_some())?;
    command
        .stdin(if key.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(if key.is_some() {
            Stdio::null()
        } else {
            Stdio::piped()
        })
        .stderr(Stdio::null());
    let mut child = command
        .spawn()
        .map_err(|_| "cannot_start_trusted_lastsecrets")?;
    let result = communicate(&mut child, key);
    if result.is_err() {
        // Only this child's PID is used. Kill before reap so a full pipe or
        // timed-out node request cannot leave a secret-bearing child running.
        terminate_and_reap(&mut child)?;
    }
    result
}

fn terminate_and_reap(child: &mut Child) -> Result<(), &'static str> {
    if child
        .try_wait()
        .map_err(|_| "cannot_inspect_owned_lastsecrets_child")?
        .is_some()
    {
        return Ok(());
    }
    let _ = child.kill();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child
            .try_wait()
            .map_err(|_| "cannot_reap_owned_lastsecrets_child")?
            .is_some()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err("cannot_reap_owned_lastsecrets_child")
}

fn communicate(child: &mut Child, key: Option<&[u8]>) -> Result<Vec<u8>, &'static str> {
    let deadline = Instant::now() + DEADLINE;
    let mut stdin = child.stdin.take();
    let mut stdout = child.stdout.take();
    if let Some(stdin) = &stdin {
        nonblocking(stdin.as_raw_fd())?;
    }
    if let Some(stdout) = &stdout {
        nonblocking(stdout.as_raw_fd())?;
    }
    let mut written = 0;
    let mut bytes = Vec::new();
    loop {
        if Instant::now() >= deadline {
            return Err("lastsecrets_result_uncertain_timeout");
        }
        if let (Some(input), Some(value)) = (&mut stdin, key) {
            match input.write(&value[written..]) {
                Ok(0) if written != value.len() => return Err("lastsecrets_stdin_write_failed"),
                Ok(count) => written += count,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => return Err("lastsecrets_stdin_write_failed"),
            }
            if written == value.len() {
                stdin = None;
            }
        }
        if let Some(output) = &mut stdout {
            read_available(output, &mut bytes)?;
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|_| "cannot_wait_for_lastsecrets")?
        {
            if !status.success() || key.is_some_and(|value| written != value.len()) {
                return Err("lastsecrets_result_uncertain_failure");
            }
            if let Some(output) = &mut stdout {
                read_available(output, &mut bytes)?;
            }
            return Ok(bytes);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn read_available(output: &mut impl Read, bytes: &mut Vec<u8>) -> Result<(), &'static str> {
    let mut buffer = [0; 512];
    loop {
        match output.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(count) if count <= READBACK_LIMIT.saturating_sub(bytes.len()) => {
                bytes.extend_from_slice(&buffer[..count]);
            }
            Ok(_) => return Err("lastsecrets_readback_exceeds_supported_size"),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
            Err(_) => return Err("cannot_read_private_lastsecrets_result"),
        }
    }
}

fn nonblocking(descriptor: RawFd) -> Result<(), &'static str> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err("cannot_bound_lastsecrets_pipe");
    }
    Ok(())
}
