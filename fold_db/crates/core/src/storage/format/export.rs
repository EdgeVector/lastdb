//! Export roles for every persisted disk part this home can produce.
//!
//! Cloud code reads this table. It does not keep a second filename or key-prefix
//! list. A new [`laststore::disk_run::home::CapturedRunKind`] has to be named
//! here or the crate does not compile. A new row is reported even when the
//! capture emits no file for it.

use laststore::disk_run::home::CapturedRunKind;

use super::{CAS_DIR, RUN_DIR};

/// What cloud is allowed to do with one persisted disk part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportRole {
    /// Frozen run-home files that restore record state.
    Snapshot,
    /// Sealed mutation-log segments. This capture does not emit them.
    ReplayLog,
    /// Frozen files from the record-owned blob home.
    RecordOwnedBlob,
    /// A live owner file. It stays on the node.
    LocalOnly,
}

/// Whether a part must appear in a frozen export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportPresence {
    /// The selected closure is incomplete when the part has no artifact.
    Required,
    /// Zero artifacts is an empty set, not a dropped part.
    EmptyOk,
    /// This capture facility does not emit the part. Absence is a gap.
    Gap,
    /// The part is named so it is not silently dropped, and it is not exported.
    LocalOnly,
}

/// One persisted part and the other parts a restorable copy of it needs.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExportPart {
    pub id: &'static str,
    pub role: ExportRole,
    pub presence: ExportPresence,
    pub reason: &'static str,
    pub depends_on: &'static [&'static str],
}

const REPLAY_GAP: &str = "sealed replay segments are not in the frozen run capture. \
MutationLog records them only after the log is enabled (index.enabled) and a snapshot \
anchor exists (begin returns None otherwise, and retain keeps the run parts). \
FrozenLogCapture seals those parts as a backup job. RunHome::capture_files does not \
pin them. This spike does not add a journal.";

/// The only classification. Order is the order cloud reports.
pub(crate) const EXPORT_PARTS: &[ExportPart] = &[
    ExportPart {
        id: "node-marker",
        role: ExportRole::Snapshot,
        presence: ExportPresence::Required,
        reason: "",
        depends_on: &[],
    },
    ExportPart {
        id: "main-descriptor",
        role: ExportRole::Snapshot,
        presence: ExportPresence::Required,
        reason: "",
        depends_on: &["node-marker"],
    },
    ExportPart {
        id: "main-current",
        role: ExportRole::Snapshot,
        presence: ExportPresence::Required,
        reason: "",
        depends_on: &["main-descriptor"],
    },
    ExportPart {
        id: "main-run",
        role: ExportRole::Snapshot,
        presence: ExportPresence::EmptyOk,
        reason: "",
        depends_on: &["main-current"],
    },
    ExportPart {
        id: "cas-descriptor",
        role: ExportRole::RecordOwnedBlob,
        presence: ExportPresence::Required,
        reason: "",
        depends_on: &["node-marker"],
    },
    ExportPart {
        id: "cas-current",
        role: ExportRole::RecordOwnedBlob,
        presence: ExportPresence::Required,
        reason: "",
        depends_on: &["cas-descriptor"],
    },
    ExportPart {
        id: "cas-run",
        role: ExportRole::RecordOwnedBlob,
        presence: ExportPresence::EmptyOk,
        reason: "",
        depends_on: &["cas-current", "main-current"],
    },
    ExportPart {
        id: "replay-segment",
        role: ExportRole::ReplayLog,
        presence: ExportPresence::Gap,
        reason: REPLAY_GAP,
        depends_on: &["main-current", "cas-current"],
    },
    ExportPart {
        id: "run-lock",
        role: ExportRole::LocalOnly,
        presence: ExportPresence::LocalOnly,
        reason: "the run-home lock is a live owner file, not a committed artifact",
        depends_on: &[],
    },
    ExportPart {
        id: "node-cleanup",
        role: ExportRole::LocalOnly,
        presence: ExportPresence::LocalOnly,
        reason: "the node cleanup checkpoint is a live recovery file, not a committed artifact",
        depends_on: &[],
    },
    ExportPart {
        id: "cas-delete-intent",
        role: ExportRole::LocalOnly,
        presence: ExportPresence::LocalOnly,
        reason: "the cross-home deletion intent is a live owner file, not a committed artifact",
        depends_on: &[],
    },
    ExportPart {
        id: "backup-hold",
        role: ExportRole::LocalOnly,
        presence: ExportPresence::LocalOnly,
        reason: "the cloud backup hold directory is local backup state, not a disk export",
        depends_on: &[],
    },
];

pub(crate) fn part_by_id(id: &str) -> Option<&'static ExportPart> {
    EXPORT_PARTS.iter().find(|part| part.id == id)
}

/// Map one captured run-home file to its part. The kind match is exhaustive.
pub(crate) fn classify(home_dir: &str, kind: CapturedRunKind) -> Option<&'static ExportPart> {
    let id = match (home_dir, kind) {
        (RUN_DIR, CapturedRunKind::Descriptor) => "main-descriptor",
        (RUN_DIR, CapturedRunKind::Current) => "main-current",
        (RUN_DIR, CapturedRunKind::Run) => "main-run",
        (CAS_DIR, CapturedRunKind::Descriptor) => "cas-descriptor",
        (CAS_DIR, CapturedRunKind::Current) => "cas-current",
        (CAS_DIR, CapturedRunKind::Run) => "cas-run",
        (_, CapturedRunKind::Descriptor | CapturedRunKind::Current | CapturedRunKind::Run) => {
            return None;
        }
    };
    part_by_id(id)
}

pub(crate) fn kind_name(kind: CapturedRunKind) -> &'static str {
    match kind {
        CapturedRunKind::Descriptor => "descriptor",
        CapturedRunKind::Current => "current",
        CapturedRunKind::Run => "run",
    }
}

/// Refuse a spec that would drop a part or give a local-only part a destination.
pub(crate) fn validate_spec() -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for part in EXPORT_PARTS {
        if !seen.insert(part.id) {
            let id = part.id;
            return Err(format!("duplicate export part {id}"));
        }
        match part.role {
            ExportRole::LocalOnly => local_only_shape(part)?,
            ExportRole::ReplayLog => replay_shape(part)?,
            ExportRole::Snapshot | ExportRole::RecordOwnedBlob => exported_shape(part)?,
        }
        for dependency in part.depends_on {
            if !EXPORT_PARTS.iter().any(|other| other.id == *dependency) {
                let id = part.id;
                return Err(format!(
                    "export part {id} depends on unknown part {dependency}"
                ));
            }
        }
    }
    Ok(())
}

fn local_only_shape(part: &ExportPart) -> Result<(), String> {
    let id = part.id;
    match part.presence {
        ExportPresence::LocalOnly if !part.reason.is_empty() => Ok(()),
        ExportPresence::LocalOnly => Err(format!("local-only part {id} needs a reason")),
        ExportPresence::Required | ExportPresence::EmptyOk | ExportPresence::Gap => {
            Err(format!("local-only part {id} has the wrong presence"))
        }
    }
}

fn replay_shape(part: &ExportPart) -> Result<(), String> {
    let id = part.id;
    match part.presence {
        ExportPresence::Gap if !part.reason.is_empty() => Ok(()),
        ExportPresence::Gap => Err(format!("replay gap {id} needs a reason")),
        ExportPresence::Required | ExportPresence::EmptyOk | ExportPresence::LocalOnly => {
            Err(format!("replay part {id} has the wrong presence"))
        }
    }
}

fn exported_shape(part: &ExportPart) -> Result<(), String> {
    let id = part.id;
    match part.presence {
        ExportPresence::Required | ExportPresence::EmptyOk if part.reason.is_empty() => Ok(()),
        ExportPresence::Required | ExportPresence::EmptyOk => {
            Err(format!("exported part {id} has a reason"))
        }
        ExportPresence::Gap | ExportPresence::LocalOnly => {
            Err(format!("exported part {id} has the wrong presence"))
        }
    }
}
