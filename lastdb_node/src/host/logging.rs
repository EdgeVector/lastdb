//! Privacy-preserving daemon event formatting and stdio log file rotation.

#[cfg(target_os = "macos")]
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

struct PrivateFormat;

#[derive(Default)]
struct Metrics(Vec<(&'static str, u64)>);

impl Visit for Metrics {
    fn record_u64(&mut self, f: &Field, v: u64) {
        if matches!(
            f.name(),
            "workers" | "queue_capacity" | "max_field_value_bytes" | "default" | "absolute_max"
        ) {
            self.0.push((f.name(), v));
        }
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        if let Ok(v) = u64::try_from(v) {
            self.record_u64(f, v);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
}

impl<S, N> FormatEvent<S, N> for PrivateFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _: &FmtContext<'_, S, N>,
        mut w: Writer<'_>,
        ev: &Event<'_>,
    ) -> fmt::Result {
        let meta = ev.metadata();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let cat = match meta.target() {
            "lastdbd::shutdown" => "shutdown",
            "lastdb_node::cloud" => "cloud",
            "observability::crash" => "panic_report",
            _ => "daemon",
        };
        write!(w, "timestamp={ts} severity={} category={cat}", meta.level())?;
        if meta.target() == "lastdbd" {
            let mut m = Metrics::default();
            ev.record(&mut m);
            for (k, v) in m.0 {
                write!(w, " {k}={v}")?;
            }
        }
        writeln!(w)
    }
}

pub fn init_daemon_logging() -> Option<observability::layers::error::SentryGuard> {
    let rot_on = !matches!(
        std::env::var("LASTDBD_STDIO_LOG_ROTATION").ok().as_deref(),
        Some("0" | "false" | "FALSE" | "off" | "OFF")
    );
    let max = fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_MAX_BYTES", 10 * 1024 * 1024u64);
    if rot_on && max > 0 {
        let keep = fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_KEEP", 5usize);
        let iv = Duration::from_secs(
            fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_CHECK_SECONDS", 60u64).max(1),
        );
        for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
            if let Some(p) = fd_regular_file_path(fd) {
                std::thread::spawn(move || loop {
                    let _ = rotate_fd_if_needed(fd, &p, max, keep);
                    std::thread::sleep(iv);
                });
            }
        }
    }
    use observability::layers::error::{build_error_layer, OBS_SENTRY_RELEASE_ENV};
    std::env::set_var(OBS_SENTRY_RELEASE_ENV, crate::version::build_version());
    let filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| tracing_subscriber::EnvFilter::try_new(v.trim()).ok())
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new("info"));
    let (err_layer, guard) = build_error_layer().unzip();
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .with_ansi(false)
        .event_format(PrivateFormat);
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(err_layer)
        .init();
    guard
}

fn rotate_fd_if_needed(fd: RawFd, path: &Path, max: u64, keep: usize) -> io::Result<()> {
    let meta = std::fs::metadata(path)?;
    if !meta.is_file() || meta.len() <= max {
        return Ok(());
    }
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("lastdbd.log");
    let rot = |i: usize| path.with_file_name(format!("{name}.{i}"));
    if keep > 0 {
        match std::fs::remove_file(rot(keep)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        for i in (2..=keep).rev().filter(|&i| rot(i - 1).exists()) {
            std::fs::rename(rot(i - 1), rot(i))?;
        }
        std::fs::copy(path, rot(1))?;
        std::fs::set_permissions(rot(1), meta.permissions())?;
    }
    // SAFETY: `fd` is one of this process's stdio descriptors; `ftruncate` and `lseek` do not consume it.
    if unsafe { libc::ftruncate(fd, 0) } != 0 || unsafe { libc::lseek(fd, 0, libc::SEEK_SET) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn fd_regular_file_path(fd: RawFd) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let mut buf = [0 as libc::c_char; libc::MAXPATHLEN as usize];
        // SAFETY: `buf` is a writable MAXPATHLEN buffer as required by F_GETPATH.
        if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } == 0 {
            // SAFETY: F_GETPATH writes a NUL-terminated path into `buf` on success.
            let p = PathBuf::from(
                unsafe { CStr::from_ptr(buf.as_ptr()) }
                    .to_string_lossy()
                    .into_owned(),
            );
            if std::fs::metadata(&p).is_ok_and(|m| m.is_file()) {
                return Some(p);
            }
        }
    }
    [format!("/dev/fd/{fd}"), format!("/proc/self/fd/{fd}")]
        .into_iter()
        .filter_map(|c| std::fs::read_link(c).ok())
        .find(|p| std::fs::metadata(p).is_ok_and(|m| m.is_file()))
}
