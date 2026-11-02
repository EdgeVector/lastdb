//! One user agent keeps registry apps current independently of Last Stack.

#[cfg(target_os = "macos")]
pub fn install() -> Result<(), String> {
    use std::{fs, path::PathBuf, process::Command};
    // Explicit alternate roots are disposable installs, not the user's app fleet.
    if std::env::var_os("LASTDB_APPS_ROOT").is_some() {
        return Ok(());
    }
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?);
    let agents = home.join("Library/LaunchAgents");
    let state = home.join(".local/state/lastdb-app-registry");
    fs::create_dir_all(&agents).map_err(|e| e.to_string())?;
    fs::create_dir_all(&state).map_err(|e| e.to_string())?;
    let label = "com.edgevector.lastdb-app-registry";
    let plist = agents.join(format!("{label}.plist"));
    let escape = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
    let user_home = escape(&home.display().to_string());
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
    let service = format!("{domain}/{label}");
    let _ = Command::new("launchctl")
        .args(["bootout", &service])
        .output();
    let result = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .output()
        .map_err(|e| e.to_string())?;
    if !result.status.success() {
        return Err("could not load registry app updater".into());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn install() -> Result<(), String> {
    Ok(())
}
