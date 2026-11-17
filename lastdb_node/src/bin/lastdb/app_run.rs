//! The app launcher. An installed app that declares LastDB access starts
//! here, on the app socket. The node serves it only the schemas it declared,
//! which keeps the record of which app uses which schema true. Nothing
//! confines the program on the machine.

use std::ffi::OsString;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use serde_json::{json, Value};

use crate::app_registry_index::launch;

const REGISTER_TIMEOUT: Duration = Duration::from_secs(5);

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
    let access = launch.receipt.listing.access.as_ref().ok_or_else(|| {
        format!("app '{app}' declares no LastDB access; start {command} directly")
    })?;
    let node_data = home.join("data");
    // The app's own folder for its files. It outlives an upgrade, because
    // versions sit beside it and not inside it. Create it before registration
    // so the node can check the directory it will later write into.
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
    let body = json!({
        "app": app,
        "access": {
            "read": access.read.keys().collect::<Vec<_>>(),
            "write": access.write.keys().collect::<Vec<_>>(),
            "extensions": access.extensions,
        },
        "data": data_text,
    });
    let raw = crate::uds_client::post_json_with_timeout(
        &lastdb_node::lastdb_uds::socket_path(&node_data),
        "/api/apps/session",
        &body,
        REGISTER_TIMEOUT,
    )?;
    let reply = crate::uds_client::parse_json_response(&raw, "app registration")?;
    let app_id = reply
        .get("app")
        .and_then(Value::as_str)
        .ok_or("the node returned no app id")?;
    let app_socket = node_data.join(lastdb_node::lastdb_uds::APP_SOCKET_FILE_NAME);
    let mut child = Command::new(&launch.program);
    child
        .env("LASTDB_SOCKET_PATH", &app_socket)
        .env("LASTDB_APP", app_id)
        .env("LASTDB_APP_DATA", &app_data)
        .env("TMPDIR", &temp)
        .env_remove("FOLDDB_SOCKET_PATH");
    Ok(child)
}
