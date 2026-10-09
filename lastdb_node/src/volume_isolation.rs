//! Explicit, privileged, opt-in isolation of a FRESH node home from macOS
//! Time Machine and FSEvents.
//!
//! Spotlight exclusion (`host::mark_never_indexed`) already happens
//! automatically on every boot and needs no privilege — see `host.rs`. Time
//! Machine and FSEvents are different:
//!
//! * `tmutil addexclusion -p <path>` requires root (verified empirically:
//!   `tmutil: addexclusion requires root privileges.`, exit 80, on a
//!   user-owned directory). It cannot run silently inside the unprivileged
//!   `lastdbd` boot path the way the Spotlight marker does.
//! * FSEvents has no per-directory opt-out at all. The only mechanism is a
//!   `.fseventsd/no_log` marker at the root of an APFS **volume**, which
//!   turns FSEvents off for everything on that volume. Giving a node home
//!   its own volume is the only way to isolate it without disabling FSEvents
//!   for the user's whole disk.
//!
//! Both of those are real, sometimes hard-to-reverse local-machine changes
//! (an owned directory's backup status; a new disk volume), so this module
//! is deliberately never called from `Host::boot` or any other automatic
//! path. It is reached only through the `lastdb isolate-volume` CLI command,
//! defaults to a dry-run plan, and refuses to touch a home that already
//! holds a store — this is for a brand new install, not a migration of a
//! live one (a live primary needs a deliberate, supervised migration, not
//! this command).
//!
//! Investigation this responds to: `investigation-20260930-fseventsd-pinned-cpu`
//! and `design-lastdb-install-avoid-fsevents-spotlight-overhead` (brain).

use std::path::{Path, PathBuf};
use std::process::Command;

/// FSEvents opt-out marker: an empty file at `<volume-root>/.fseventsd/no_log`
/// disables FSEvents logging for the whole volume.
const FSEVENTS_NO_LOG_DIR: &str = ".fseventsd";
const FSEVENTS_NO_LOG_FILE: &str = "no_log";

/// What would be created for the optional dedicated APFS volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumePlan {
    /// APFS container the new volume is added to (e.g. `disk3`) — the same
    /// container backing the reference path, so the volume lands on the same
    /// physical disk as the data it isolates rather than an unrelated one.
    pub container: String,
    pub volume_name: String,
    /// Where the new volume will mount. APFS volumes in the boot container
    /// mount at `/Volumes/<name>` by default and remount automatically on
    /// restart, so no fstab/launchd entry is needed.
    pub mount_point: PathBuf,
}

/// The full isolation plan for one node home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolationPlan {
    pub home: PathBuf,
    pub volume: Option<VolumePlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolationReport {
    pub time_machine_excluded: bool,
    /// The node home actually initialized: `home` when no volume was
    /// requested, or the new volume's mount point when one was.
    pub effective_home: PathBuf,
}

/// Read `diskutil info -plist <path>` and pull one string-valued key.
///
/// A hand-rolled scan over the handful of flat `<key>K</key><string>V</string>`
/// pairs this module reads, rather than a general plist parser: no plist
/// crate is in this workspace's dependency tree today, and pulling one in
/// for three fields is not proportional to the need.
fn plist_string_value(xml: &str, key: &str) -> Option<String> {
    let key_tag = format!("<key>{key}</key>");
    let after_key = &xml[xml.find(&key_tag)? + key_tag.len()..];
    let after_open = &after_key[after_key.find("<string>")? + "<string>".len()..];
    let close = after_open.find("</string>")?;
    Some(after_open[..close].trim().to_string())
}

fn run(cmd: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| format!("failed to run `{cmd} {}`: {e}", args.join(" ")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "`{cmd} {}` failed: {}",
            args.join(" "),
            stderr.trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| format!("`{cmd} {}` produced non-UTF8 output: {e}", args.join(" ")))
}

/// `diskutil ... -plist` reports its own failures as an `<Error>true</Error>`
/// / `<ErrorMessage>` pair inside the plist on **stdout**, with an empty
/// stderr (verified empirically: `diskutil info -plist <arbitrary-subdir>`
/// exits 1 with nothing on stderr and the reason in the plist body) — so a
/// bare stderr-only failure message would read as `` failed: `` with nothing
/// after it. This runs the command and surfaces the plist's own
/// `ErrorMessage` when present, falling back to stderr otherwise.
fn run_diskutil_plist(args: &[&str]) -> Result<String, String> {
    let output = Command::new("diskutil")
        .args(args)
        .output()
        .map_err(|e| format!("failed to run `diskutil {}`: {e}", args.join(" ")))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.status.success() {
        let reason = plist_string_value(&stdout, "ErrorMessage")
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| String::from_utf8_lossy(&output.stderr).trim().to_string());
        return Err(format!("`diskutil {}` failed: {reason}", args.join(" ")));
    }
    Ok(stdout)
}

/// The device node backing `path` (e.g. `/dev/disk3s5`), via `df`.
///
/// `diskutil info -plist <path>` only resolves an actual mount point, not an
/// arbitrary file or subdirectory beneath one (verified empirically: it
/// resolves `/System/Volumes/Data` but fails on `$HOME`, a firmlinked
/// subdirectory of that mount, with "Could not find disk"). `df` resolves any
/// path to the device backing it, so this goes through `df` first.
fn device_node_for_path(path: &Path) -> Result<String, String> {
    let output = run("df", &["-P", &path.to_string_lossy()])?;
    let device = output
        .lines()
        .nth(1)
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| {
            format!(
                "could not parse `df -P {}` output: {output:?}",
                path.display()
            )
        })?;
    Ok(device.to_string())
}

/// The APFS container backing `reference_path`, so a new volume lands next
/// to the data it is meant to isolate.
fn detect_container(reference_path: &Path) -> Result<String, String> {
    let device = device_node_for_path(reference_path)?;
    let xml = run_diskutil_plist(&["info", "-plist", &device])?;
    plist_string_value(&xml, "APFSContainerReference").ok_or_else(|| {
        format!(
            "could not determine the APFS container for {} ({device}): no \
             APFSContainerReference in `diskutil info -plist` output (is it on an APFS volume?)",
            reference_path.display()
        )
    })
}

/// Whether this process is running as root. Both operations this module
/// performs (`tmutil addexclusion`, APFS volume creation) need root; refusing
/// up front with one clear message beats a partial failure halfway through.
fn running_as_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

/// Build the isolation plan, without touching the disk or requiring root —
/// this is what `--execute`-less invocations print.
pub fn plan(home: &Path, volume_name: &str, skip_volume: bool) -> Result<IsolationPlan, String> {
    if !cfg!(target_os = "macos") {
        return Err(
            "isolate-volume is macOS-only: Time Machine and FSEvents, the things it isolates \
             from, are macOS mechanisms"
                .to_string(),
        );
    }
    if crate::host::data_dir_has_existing_store(&home.join("data")) {
        return Err(format!(
            "{} already holds a store; isolate-volume is for a fresh home only. Point it at a \
             new, not-yet-initialized home instead — a live home needs a deliberate, supervised \
             migration, not this command.",
            home.display()
        ));
    }

    let volume = if skip_volume {
        None
    } else {
        // The container is detected from the home's parent (or $HOME as a
        // fallback for a home path that does not exist yet), so the new
        // volume shares a physical disk with the rest of the user's data
        // rather than defaulting to whichever disk `diskutil` picks.
        let reference: PathBuf = home
            .parent()
            .filter(|p| p.exists())
            .map(Path::to_path_buf)
            .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
            .ok_or_else(|| "could not find a reference path to detect the APFS container from (neither the home's parent nor $HOME exist)".to_string())?;
        let container = detect_container(&reference)?;
        Some(VolumePlan {
            container,
            volume_name: volume_name.to_string(),
            mount_point: PathBuf::from(format!("/Volumes/{volume_name}")),
        })
    };

    Ok(IsolationPlan {
        home: home.to_path_buf(),
        volume,
    })
}

/// Carry out a plan: exclude from Time Machine, and — if the plan includes
/// one — create the dedicated APFS volume, disable FSEvents on it, and
/// initialize the node home there. Real, disk- and backup-affecting `tmutil`
/// / `diskutil` calls; only reached when the caller passed `--execute`.
pub fn execute(plan: &IsolationPlan) -> Result<IsolationReport, String> {
    if !running_as_root() {
        return Err(
            "isolate-volume --execute needs root (both `tmutil addexclusion` and APFS volume \
             creation require it): re-run with sudo"
                .to_string(),
        );
    }

    std::fs::create_dir_all(&plan.home)
        .map_err(|e| format!("failed to create {}: {e}", plan.home.display()))?;
    run(
        "tmutil",
        &["addexclusion", "-p", &plan.home.to_string_lossy()],
    )?;

    let effective_home = match &plan.volume {
        None => plan.home.clone(),
        Some(v) => {
            run(
                "diskutil",
                &["apfs", "addVolume", &v.container, "APFS", &v.volume_name],
            )?;

            if !v.mount_point.is_dir() {
                return Err(format!(
                    "diskutil reported success but {} does not exist; check \
                     `diskutil apfs list {}` by hand",
                    v.mount_point.display(),
                    v.container
                ));
            }

            let no_log_dir = v.mount_point.join(FSEVENTS_NO_LOG_DIR);
            std::fs::create_dir_all(&no_log_dir)
                .map_err(|e| format!("failed to create {}: {e}", no_log_dir.display()))?;
            let no_log_file = no_log_dir.join(FSEVENTS_NO_LOG_FILE);
            if !no_log_file.exists() {
                std::fs::write(&no_log_file, b"")
                    .map_err(|e| format!("failed to write {}: {e}", no_log_file.display()))?;
            }

            v.mount_point.clone()
        }
    };

    crate::host::ensure_node_home(&effective_home)
        .map_err(|e| format!("failed to initialize node home at {effective_home:?}: {e}"))?;

    Ok(IsolationReport {
        time_machine_excluded: true,
        effective_home,
    })
}
