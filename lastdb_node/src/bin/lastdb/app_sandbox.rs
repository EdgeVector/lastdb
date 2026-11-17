//! The sandbox profile for one restricted app.
//!
//! The launcher and the installer both build the profile here, so the two
//! cannot drift. `install_source` is `None` for launch. The install profile
//! is that same text plus writes inside the source folder of the version the
//! step belongs to.

#[cfg(target_os = "macos")]
use std::path::Path;

/// The profile of one app. It takes away the LastDB home, every other
/// installed app, the owner socket, and the ways one process reads another.
/// It also takes away every write: an app writes only inside its own data
/// folder. Without that an app could replace a command or a config file of
/// the owner and run outside the profile the next time that file is used.
///
/// When `install_source` is set, an install step may also write inside that
/// one source folder. Nothing else is added. The rest of the app's install
/// tree stays read-only: it holds the receipt that says the app is restricted
/// and the link to the current version, and a step that could write them
/// could take the app out of its restriction.
#[cfg(target_os = "macos")]
pub(crate) fn profile(
    home: &Path,
    app: &str,
    app_socket: &Path,
    app_data: &Path,
    install_source: Option<&Path>,
) -> String {
    let quoted = |path: &Path| {
        let real = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let text = real.to_string_lossy();
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    };
    // The app's own install tree stays readable; every other app's does not.
    let own = crate::app_registry_index::managed::app_root(app).unwrap_or_default();
    let apps = own.parent().unwrap_or(&own);
    let base = [
        "(version 1)".to_owned(),
        "(allow default)".to_owned(),
        format!("(deny file-read* file-write* (subpath {}))", quoted(home)),
        format!("(deny file-read* file-write* (subpath {}))", quoted(apps)),
        format!("(allow file-read* (subpath {}))", quoted(&own)),
        // `mkdir -p` and path resolution stat every folder on the way to the
        // app's own tree. Without this a step that creates a folder inside
        // its data folder fails on the apps root itself. Only the metadata
        // of that one folder is readable: its listing, which names the
        // other installed apps, stays denied.
        format!("(allow file-read-metadata (literal {}))", quoted(apps)),
        "(deny file-write* (subpath \"/\"))".to_owned(),
        format!("(allow file-write* (subpath {}))", quoted(app_data)),
        "(allow file-write* (literal \"/dev/null\") (literal \"/dev/tty\") \
         (literal \"/dev/dtracehelper\") (regex #\"^/dev/ttys[0-9]+$\"))"
            .to_owned(),
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
    .join("\n");
    match install_source {
        Some(source) => {
            let rule = format!("(allow file-write* (subpath {}))", quoted(source));
            format!("{base}\n{rule}")
        }
        None => base,
    }
}
