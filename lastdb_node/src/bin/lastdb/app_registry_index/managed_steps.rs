//! Install steps. An unrestricted app runs them as before.
//!
//! A restricted app does not receive the owner socket or an app session.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::managed_manifest::InstallStep;
use super::str_err;

const PASS: &str =
    "HOME PATH TMPDIR USER LOGNAME SSH_AUTH_SOCK LASTDB_APPS_ROOT LANG LAST_STACK_APPS_OFF";

/// Run `prepare`, `probe`, or `post_install`. `restricted` follows the listing:
/// the app declares LastDB access. Timeouts, accepted exits, and errors stay
/// as they are.
pub(super) fn steps(
    source: &Path,
    home: &Path,
    app: &str,
    list: &[InstallStep],
    restricted: bool,
) -> Result<(), String> {
    for step in list {
        run(source, home, app, step, restricted)?;
    }
    Ok(())
}

fn run(
    source: &Path,
    home: &Path,
    app: &str,
    step: &InstallStep,
    restricted: bool,
) -> Result<(), String> {
    let mut cmd = command(source, home, app, step, restricted)?;
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
    Ok(())
}

fn command(
    source: &Path,
    home: &Path,
    app: &str,
    step: &InstallStep,
    restricted: bool,
) -> Result<Command, String> {
    let paths = if restricted {
        Some(app_paths(home, app)?)
    } else {
        None
    };
    let mut cmd = Command::new(&step.argv[0]);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        cmd.process_group(0);
    }
    cmd.args(&step.argv[1..]).current_dir(source).env_clear();
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(std::io::stderr()))
        .stderr(Stdio::inherit());
    cmd.envs(
        PASS.split_whitespace()
            .filter_map(|k| Some((k, std::env::var_os(k)?))),
    );
    let data = home.join("data");
    cmd.env("LASTDB_HOME", home).env("LASTDB_DATA_DIR", &data);
    if let Some(paths) = &paths {
        cmd.env("LASTDB_SOCKET_PATH", &paths.socket)
            .env("LASTDB_APP_DATA", &paths.data)
            .env("TMPDIR", &paths.tmp)
            .env_remove("LASTDB_SOCKET")
            .env_remove("FOLDDB_SOCKET_PATH");
    } else {
        let sock = lastdb_node::lastdb_uds::socket_path(&data);
        cmd.env("LASTDB_SOCKET", &sock);
    }
    cmd.env("LASTDB_APP_ID", app)
        .env("LASTDB_APP_SOURCE_DIR", source)
        .env("LAST_STACK_SETUP_PRESERVE_SERVICES", "1");
    Ok(cmd)
}

struct AppPaths {
    socket: PathBuf,
    data: PathBuf,
    tmp: PathBuf,
}

/// The app data folder and its `tmp`, created the way the launcher creates them.
fn app_paths(home: &Path, app: &str) -> Result<AppPaths, String> {
    let socket = home
        .join("data")
        .join(lastdb_node::lastdb_uds::APP_SOCKET_FILE_NAME);
    let data = super::managed::app_root(app)?.join("data");
    let tmp = data.join("tmp");
    std::fs::create_dir_all(&tmp)
        .map_err(|error| format!("could not create {}: {error}", tmp.display()))?;
    Ok(AppPaths { socket, data, tmp })
}
