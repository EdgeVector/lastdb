//! The app launcher. An installed app that declares LastDB access starts
//! here: with an app session, on the app socket, and on macOS inside a
//! sandbox profile that denies it the LastDB home and the owner socket.

use std::ffi::OsString;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use serde_json::{json, Value};

use crate::app_registry_index::launch::{self, Launch};

const SESSION_TIMEOUT: Duration = Duration::from_secs(5);

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
    let data = home.join("data");
    let body = json!({
        "app": app,
        "access": {
            "read": access.read.keys().collect::<Vec<_>>(),
            "write": access.write.keys().collect::<Vec<_>>(),
            "extensions": access.extensions,
        },
    });
    let raw = crate::uds_client::post_json_with_timeout(
        &lastdb_node::lastdb_uds::socket_path(&data),
        "/api/apps/session",
        &body,
        SESSION_TIMEOUT,
    )?;
    let reply = crate::uds_client::parse_json_response(&raw, "app session")?;
    let session = reply
        .get("session")
        .and_then(Value::as_str)
        .ok_or("the node returned no app session")?;
    let app_socket = data.join(lastdb_node::lastdb_uds::APP_SOCKET_FILE_NAME);
    let mut child = restricted(home, app, &launch, &app_socket);
    child
        .env("LASTDB_SOCKET_PATH", &app_socket)
        .env("LASTDB_APP_SESSION", session)
        .env_remove("FOLDDB_SOCKET_PATH");
    Ok(child)
}

#[cfg(target_os = "macos")]
fn restricted(home: &Path, app: &str, launch: &Launch, app_socket: &Path) -> Command {
    let mut child = Command::new("/usr/bin/sandbox-exec");
    child
        .arg("-p")
        .arg(profile(home, app, app_socket))
        .arg(&launch.program);
    child
}

/// No sandbox tool is chosen for this platform yet. The app still gets only
/// the app socket and a session, which holds an app that does not try to get
/// around it.
#[cfg(not(target_os = "macos"))]
fn restricted(_home: &Path, _app: &str, launch: &Launch, _app_socket: &Path) -> Command {
    Command::new(&launch.program)
}

/// The profile of one app. It starts from everything allowed and takes away
/// the LastDB home, every other installed app, the owner socket, and the ways
/// one process reads another.
#[cfg(target_os = "macos")]
fn profile(home: &Path, app: &str, app_socket: &Path) -> String {
    let quoted = |path: &Path| {
        let real = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let text = real.to_string_lossy();
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    };
    // The app's own install tree stays readable; every other app's does not.
    let own = crate::app_registry_index::managed::app_root(app).unwrap_or_default();
    let apps = own.parent().unwrap_or(&own);
    [
        "(version 1)".to_owned(),
        "(allow default)".to_owned(),
        format!("(deny file-read* file-write* (subpath {}))", quoted(home)),
        format!("(deny file-read* file-write* (subpath {}))", quoted(apps)),
        format!("(allow file-read* (subpath {}))", quoted(&own)),
        format!(
            "(deny network-outbound (remote unix-socket (subpath {})))",
            quoted(home)
        ),
        format!(
            "(allow network-outbound (remote unix-socket (path-literal {})))",
            quoted(app_socket)
        ),
        "(deny process-info* (target others))".to_owned(),
        "(deny signal (target others))".to_owned(),
        "(deny mach-priv-task-port)".to_owned(),
        "(deny sysctl-read (sysctl-name-prefix \"kern.procargs\"))".to_owned(),
    ]
    .join("\n")
}
