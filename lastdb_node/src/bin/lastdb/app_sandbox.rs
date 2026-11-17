//! The sandbox profile for one restricted app.
//!
//! The launcher and the installer both build the profile here, so the two
//! cannot drift. `install_tree_writable` is false for launch. The install
//! profile is that same text plus writes inside the app's own install tree.

#[cfg(target_os = "macos")]
use std::path::Path;

/// The profile of one app. It takes away the LastDB home, every other
/// installed app, the owner socket, and the ways one process reads another.
/// It also takes away every write: an app writes only inside its own data
/// folder. Without that an app could replace a command or a config file of
/// the owner and run outside the profile the next time that file is used.
///
/// When `install_tree_writable` is set, a prepare step may also write inside
/// the app's own install tree. Nothing else is added.
#[cfg(target_os = "macos")]
pub(crate) fn profile(
    home: &Path,
    app: &str,
    app_socket: &Path,
    app_data: &Path,
    install_tree_writable: bool,
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
    if install_tree_writable {
        let rule = format!("(allow file-write* (subpath {}))", quoted(&own));
        format!("{base}\n{rule}")
    } else {
        base
    }
}
