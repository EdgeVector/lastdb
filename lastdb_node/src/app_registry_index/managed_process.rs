//! Bounded install commands. Child output never contaminates CLI JSON.

use super::managed_manifest::InstallStep;
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub fn steps(source: &Path, home: &Path, app: &str, list: &[InstallStep]) -> Result<(), String> {
    for step in list {
        run(source, home, app, step)?;
    }
    Ok(())
}

fn run(source: &Path, home: &Path, app: &str, step: &InstallStep) -> Result<(), String> {
    let mut cmd = Command::new(&step.argv[0]);
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
    ] {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
        }
    }
    cmd.env("LASTDB_HOME", home)
        .env("LASTDB_DATA_DIR", home.join("data"))
        .env(
            "LASTDB_SOCKET",
            lastdb_uds::uds::socket_path(&home.join("data")),
        )
        .env("LASTDB_APP_ID", app)
        .env("LASTDB_APP_SOURCE_DIR", source)
        .env("LAST_STACK_SETUP_PRESERVE_SERVICES", "1");
    if let Some(value) = std::env::var_os("LAST_STACK_APPS_OFF") {
        cmd.env("LAST_STACK_APPS_OFF", value);
    }
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
            let _ = Command::new("/bin/kill")
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
