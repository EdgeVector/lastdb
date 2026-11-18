//! The app launcher. An installed app that declares LastDB access starts
//! here, on the app socket. The node serves it the schemas its grant allows,
//! which keeps the record of which app uses which schema true. The grant was
//! stored when this version was activated. This launcher does not call the
//! node. Nothing confines the program on the machine.

use std::ffi::OsString;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Command;

use crate::app_registry_index::launch;

/// When this program was started under the command name of a restricted app
/// (its command link points here), run that app and never return. `None`
/// means this is an ordinary `lastdb` call.
pub(crate) fn run_if_app_command() -> Option<String> {
    let mut args = std::env::args_os();
    let invoked = args.next()?;
    let name = Path::new(&invoked).file_name()?.to_str()?.to_owned();
    if matches!(name.as_str(), "lastdb" | "folddb") {
        return None;
    }
    let app = launch::restricted_owner(&name)?;
    let home = match lastdb_node::host::resolve_home(None) {
        Ok(home) => home,
        Err(error) => return Some(error),
    };
    Some(run(&home, &app, &name, args.collect()))
}

/// Start `command` of `app` restricted. Returns only the reason it could not start.
pub(crate) fn run(home: &Path, app: &str, command: &str, args: Vec<OsString>) -> String {
    match prepare(home, app, command) {
        Ok(mut child) => {
            let error = child.args(args).exec();
            format!("could not start {command}: {error}")
        }
        Err(error) => error,
    }
}

fn prepare(home: &Path, app: &str, command: &str) -> Result<Command, String> {
    let launch = launch::resolve(app, command)?;
    if launch.receipt.listing.access.is_none() {
        return Err(format!(
            "app '{app}' declares no LastDB access; start {command} directly"
        ));
    }
    // Install registers this id, and the node echoes the id it was sent.
    let app_id = launch.receipt.app_id.clone();
    let node_data = home.join("data");
    // The app's own folder for its files. It outlives an upgrade, because
    // versions sit beside it and not inside it.
    let app_data = crate::app_registry_index::managed::app_root(app)?.join("data");
    let data_text = app_data.to_str().ok_or_else(|| {
        format!(
            "the app data folder must be an absolute path: {}",
            app_data.display()
        )
    })?;
    if !app_data.is_absolute() {
        return Err(format!(
            "the app data folder must be an absolute path: {data_text}"
        ));
    }
    if std::fs::symlink_metadata(&app_data)
        .is_ok_and(|meta| meta.file_type().is_symlink() || !meta.is_dir())
    {
        return Err("the app data folder must be a directory and not a symlink".into());
    }
    let temp = app_data.join("tmp");
    std::fs::create_dir_all(&temp)
        .map_err(|error| format!("could not create {}: {error}", temp.display()))?;
    let app_socket = node_data.join(lastdb_node::lastdb_uds::APP_SOCKET_FILE_NAME);
    let mut child = Command::new(&launch.program);
    child
        .env("LASTDB_SOCKET_PATH", &app_socket)
        .env("LASTDB_APP", &app_id)
        .env("LASTDB_APP_DATA", &app_data)
        .env("TMPDIR", &temp)
        .env_remove("FOLDDB_SOCKET_PATH");
    Ok(child)
}
