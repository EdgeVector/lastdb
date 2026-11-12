//! LaunchAgent updater and bounded install step execution.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::managed_manifest::InstallStep;

#[cfg(target_os = "macos")]
pub fn install() -> Result<(), String> {
    use std::{fs, path::PathBuf};
    if std::env::var_os("LASTDB_APPS_ROOT").is_some() {
        return Ok(());
    }
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?);
    let agents = home.join("Library/LaunchAgents");
    fs::create_dir_all(&agents).map_err(|e| e.to_string())?;
    fs::create_dir_all(home.join(".local/state/lastdb-app-registry")).map_err(|e| e.to_string())?;
    let label = "com.edgevector.lastdb-app-registry";
    let plist = agents.join(format!("{label}.plist"));
    let user_home = home
        .display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let apps_off = std::env::var("LAST_STACK_APPS_OFF").unwrap_or_else(|_| "0".into());
    if apps_off != "0" && apps_off != "1" {
        return Err("LAST_STACK_APPS_OFF must be 0 or 1".into());
    }
    let data = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{label}</string>
<key>ProgramArguments</key><array><string>{user_home}/.local/bin/lastdb</string><string>app</string><string>upgrade</string><string>--all</string></array>
<key>StartInterval</key><integer>60</integer>
<key>EnvironmentVariables</key><dict><key>HOME</key><string>{user_home}</string><key>PATH</key><string>{user_home}/.local/bin:{user_home}/.bun/bin:{user_home}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string><key>LASTDB_APP_REGISTRY_POLL</key><string>1</string><key>LAST_STACK_APPS_OFF</key><string>{apps_off}</string></dict>
<key>StandardOutPath</key><string>{user_home}/.local/state/lastdb-app-registry/upgrade.log</string>
<key>StandardErrorPath</key><string>{user_home}/.local/state/lastdb-app-registry/upgrade.err</string>
</dict></plist>
"#
    );
    if fs::read_to_string(&plist).ok().as_deref() == Some(&data) {
        return Ok(());
    }
    fs::write(&plist, data).map_err(|e| e.to_string())?;
    let uid = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|e| e.to_string())?;
    let domain = format!("gui/{}", String::from_utf8_lossy(&uid.stdout).trim());
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("{domain}/{label}")])
        .output();
    let ok = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .output()
        .map_err(|e| e.to_string())?
        .status
        .success();
    ok.then_some(())
        .ok_or_else(|| "could not load registry app updater".into())
}

#[cfg(not(target_os = "macos"))]
pub fn install() -> Result<(), String> {
    Ok(())
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
        "LAST_STACK_APPS_OFF",
    ] {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
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
