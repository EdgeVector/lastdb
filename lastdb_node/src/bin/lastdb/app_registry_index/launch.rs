//! What the app launcher needs from an install: the receipt, the program
//! behind a command, and which restricted app owns a command name.

use std::path::PathBuf;

use super::managed::{self, ManagedReceipt};
use super::managed_manifest::InstallPlan;
use super::{str_err, valid_app_id};

/// One command of one installed app.
pub struct Launch {
    pub receipt: ManagedReceipt,
    /// The program the command runs, inside the active version.
    pub program: PathBuf,
}

/// The installed app `app` and the program behind its command `command`.
pub fn resolve(app: &str, command: &str) -> Result<Launch, String> {
    let root = managed::app_root(app)?;
    let current = root.join("current");
    let receipt = managed::receipt(&current)
        .map_err(|error| format!("app '{app}' is not installed: {error}"))?;
    let source = current.join("source");
    let plan = InstallPlan::load(&source, app)?;
    let link = plan
        .links
        .iter()
        .find(|link| link.command == command)
        .ok_or_else(|| format!("app '{app}' has no command '{command}'"))?;
    let program = std::fs::canonicalize(source.join(&link.source)).map_err(str_err)?;
    Ok(Launch { receipt, program })
}

/// The app that owns the command name `command` and declares LastDB access.
/// An app that declares none is started directly and is not listed here.
pub fn restricted_owner(command: &str) -> Option<String> {
    let entries = std::fs::read_dir(managed::apps_root().ok()?).ok()?;
    entries.flatten().find_map(|entry| {
        let app = entry.file_name().to_str()?.to_owned();
        if !valid_app_id(&app) {
            return None;
        }
        let current = entry.path().join("current");
        if managed::receipt(&current).ok()?.listing.access.is_none() {
            return None;
        }
        let plan = InstallPlan::load(&current.join("source"), &app).ok()?;
        plan.links
            .iter()
            .any(|link| link.command == command)
            .then_some(app)
    })
}
