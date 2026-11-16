//! The macOS launch agent that keeps registry apps current.

#[cfg(target_os = "macos")]
pub(super) fn install_poll() -> Result<(), String> {
    use std::path::PathBuf;
    use std::process::Command;
    if std::env::var_os("LASTDB_APPS_ROOT").is_some() {
        return Ok(());
    }
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?);
    let agents = home.join("Library/LaunchAgents");
    for d in [&agents, &home.join(".local/state/lastdb-app-registry")] {
        std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
    }
    let label = "com.edgevector.lastdb-app-registry";
    let plist = agents.join(format!("{label}.plist"));
    let h = home
        .display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let off = std::env::var("LAST_STACK_APPS_OFF").unwrap_or_else(|_| "0".into());
    if off != "0" && off != "1" {
        return Err("LAST_STACK_APPS_OFF must be 0 or 1".into());
    }
    let data = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{label}</string>\n<key>ProgramArguments</key><array><string>{h}/.local/bin/lastdb</string><string>app</string><string>upgrade</string><string>--all</string></array>\n<key>StartInterval</key><integer>60</integer>\n<key>EnvironmentVariables</key><dict><key>HOME</key><string>{h}</string><key>PATH</key><string>{h}/.local/bin:{h}/.bun/bin:{h}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string><key>LASTDB_APP_REGISTRY_POLL</key><string>1</string><key>LAST_STACK_APPS_OFF</key><string>{off}</string></dict>\n<key>StandardOutPath</key><string>{h}/.local/state/lastdb-app-registry/upgrade.log</string>\n<key>StandardErrorPath</key><string>{h}/.local/state/lastdb-app-registry/upgrade.err</string></dict></plist>\n"
    );
    if std::fs::read_to_string(&plist).ok().as_deref() == Some(&data) {
        return Ok(());
    }
    std::fs::write(&plist, data).map_err(|e| e.to_string())?;
    let uid = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|e| e.to_string())?;
    let dom = format!("gui/{}", String::from_utf8_lossy(&uid.stdout).trim());
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("{dom}/{label}")])
        .output();
    if !Command::new("launchctl")
        .args(["bootstrap", &dom])
        .arg(&plist)
        .output()
        .map_err(|e| e.to_string())?
        .status
        .success()
    {
        return Err("could not load registry app updater".into());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub(super) fn install_poll() -> Result<(), String> {
    Ok(())
}
