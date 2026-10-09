use super::*;

/// Build identity of the daemon answering this status, plus the phase
/// vocabulary it knows.
///
/// Exists because a CLI older than the daemon renders a *silently truncated*
/// phase breakdown, and a truncated breakdown is not merely incomplete — it
/// inverts conclusions. `PhaseTimings` marks every field `#[serde(default)]`
/// and does not `deny_unknown_fields`, and `lastdb ops` renders through the
/// CLI's own compiled [`crate::request_telemetry::PhaseTimings::PHASE_NAMES`]
/// (and projects the durable rollup through the same list). So a phase the
/// daemon reports but the CLI has no field for is dropped on both paths, and
/// a dropped phase renders byte-identically to a phase measured at zero. An
/// operator reading `molecule_gate` as absent concludes "that gate is not
/// contending" — the exact opposite of what the daemon measured.
///
/// The remedy is not a version comparison (versions are opaque strings, and
/// "newer" does not tell you *which* phase went missing). The daemon
/// publishes the vocabulary it actually knows; the CLI diffs that against its
/// own and names the phases it is unable to show.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildHealth {
    /// `FOLDDB_BUILD_VERSION` of the running daemon — the same string
    /// `lastdbd --version` prints, so an operator can compare it to their
    /// `lastdb --version` directly.
    #[serde(default)]
    pub version: String,
    /// Phase names this daemon can report, in pipeline order. A client that
    /// does not recognize an entry here cannot render it and must say so.
    #[serde(default)]
    pub phase_names: Vec<String>,
}

impl BuildHealth {
    /// This binary's own build identity and phase vocabulary.
    pub(super) fn current() -> Self {
        Self {
            version: crate::crash_attribution::build_version().to_string(),
            phase_names: crate::request_telemetry::PhaseTimings::PHASE_NAMES
                .iter()
                .map(|n| (*n).to_string())
                .collect(),
        }
    }
}

/// Warn when this client cannot faithfully render the daemon's phase
/// breakdown. Returns the lines to print above `lastdb ops` / `lastdb status`
/// output; empty when the vocabularies agree.
///
/// Ordered by how badly a reader is misled:
///
/// 1. **Daemon reports phases this client has no field for** — the dangerous
///    case. Those phases are dropped by `PhaseTimings`' permissive
///    deserialize *and* projected away by the rollup query, and a dropped
///    phase is visually identical to one measured at zero. Named explicitly,
///    because "upgrade your CLI" without naming `molecule_gate` still leaves
///    the operator to guess whether the number they just read was real.
/// 2. **Daemon predates `build.phase_names`** — no vocabulary to compare, so
///    the client can only say the breakdown is unverifiable.
/// 3. **This client knows phases the daemon does not report** — benign: the
///    daemon simply never measures them and they render as absent, with no
///    misleading label attached. Reported as a note, not a warning.
#[must_use]
pub fn phase_vocabulary_skew_lines(build: &BuildHealth) -> Vec<String> {
    let client_version = crate::crash_attribution::build_version();
    let daemon_version = if build.version.is_empty() {
        "unknown"
    } else {
        build.version.as_str()
    };

    if build.phase_names.is_empty() {
        return vec![format!(
            "WARNING: daemon ({daemon_version}) does not publish its phase vocabulary; \
             this CLI ({client_version}) cannot verify the phase breakdown is complete. \
             A phase this CLI lacks a field for is dropped silently and reads as zero."
        )];
    }

    let known: std::collections::BTreeSet<&str> =
        crate::request_telemetry::PhaseTimings::PHASE_NAMES
            .iter()
            .copied()
            .collect();
    let daemon: std::collections::BTreeSet<&str> =
        build.phase_names.iter().map(String::as_str).collect();

    let mut lines = Vec::new();
    let unrenderable: Vec<&str> = daemon.difference(&known).copied().collect();
    if !unrenderable.is_empty() {
        lines.push(format!(
            "WARNING: daemon ({daemon_version}) reports {} phase(s) this CLI ({client_version}) \
             cannot render: {}. They are OMITTED below and an omitted phase looks exactly like \
             a zero one — do not read their absence as 'not contending'. Upgrade the CLI.",
            unrenderable.len(),
            unrenderable.join(", ")
        ));
    }
    let unreported: Vec<&str> = known.difference(&daemon).copied().collect();
    if !unreported.is_empty() {
        lines.push(format!(
            "Note: this CLI ({client_version}) knows {} phase(s) the daemon ({daemon_version}) \
             does not report: {}. They are never measured, not measured as zero.",
            unreported.len(),
            unreported.join(", ")
        ));
    }
    lines
}

/// How the build serving this socket compares to the CLI printing the output.
///
/// The distinction exists because **installing a `lastdbd` does not make it the
/// running one**: the install helper deliberately never restarts the daemon, so
/// between an install and its supervised restart the on-disk binary and the live
/// process are different builds. Nothing on the filesystem shows that — the new
/// file takes the old path, and `lsof` prints the running text with no
/// `(deleted)` marker, so the most natural check silently reports the *new*
/// file. Only the process can answer, and only if it was built to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildAgreement {
    /// Daemon and CLI report the same build.
    Agree,
    /// Daemon and CLI report different builds.
    Mismatch,
    /// Daemon predates `build.version` and cannot be asked what it is running.
    DaemonSilent,
}

impl BuildAgreement {
    /// Stable machine token for `--json` consumers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Agree => "agree",
            Self::Mismatch => "mismatch",
            Self::DaemonSilent => "daemon_silent",
        }
    }

    /// Classify `build` against the build version compiled into this binary.
    pub fn classify(build: &BuildHealth) -> Self {
        if build.version.is_empty() {
            Self::DaemonSilent
        } else if build.version == crate::crash_attribution::build_version() {
            Self::Agree
        } else {
            Self::Mismatch
        }
    }
}

/// Name the build actually serving the socket, and flag it when that is not the
/// build this CLI was compiled from.
///
/// Printed **above** any measurement, because a version read after a number has
/// already been attributed to the wrong binary. On 2026-08-05 the primary ran
/// `0.23.2-409` for three hours while `lastdb --version`, `lastdbd --version`,
/// the installed file and a cutover notice all reported `0.23.3-116` — no
/// surface disagreed, so every measurement taken in that window was filed
/// against a binary that was never executing. A safe-upgrade run reading the
/// CLI would likewise compute its hop from `116` and probe an upgrade path the
/// live node does not take.
///
/// The mismatch case is **not** an instruction to restart: a staged binary
/// awaiting a supervised restart is the install helper working as designed.
/// Restarting the primary to resolve a version question is never the fix.
pub fn build_identity_lines(build: &BuildHealth) -> Vec<String> {
    let client_version = crate::crash_attribution::build_version();
    match BuildAgreement::classify(build) {
        BuildAgreement::Agree => {
            vec![format!("Build:  {client_version} (daemon and CLI agree)")]
        }
        BuildAgreement::Mismatch => vec![
            format!(
                "Build:  daemon is running {}, this CLI is {client_version}",
                build.version
            ),
            "WARNING: the daemon serving this socket is NOT the build this CLI reports. \
             Attribute measurements, and compute upgrade deltas, from the daemon line \
             above — not from `lastdb --version` and not from the installed file, which \
             is staged until a supervised restart picks it up. Do not restart to resolve \
             this."
                .to_string(),
        ],
        BuildAgreement::DaemonSilent => vec![
            format!(
                "Build:  daemon unknown (predates build.version), this CLI is {client_version}"
            ),
            "WARNING: this daemon cannot report what it is running, so its build is \
             UNKNOWABLE from the node. The installed file is not evidence — it can be \
             replaced under a running process without changing the path `lsof` prints. \
             Do not attribute measurements or upgrade deltas to this CLI's version."
                .to_string(),
        ],
    }
}
