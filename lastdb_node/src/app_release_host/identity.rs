use super::*;

// ─── Execution identity ───────────────────────────────────────────────────

/// Which of the host's two execution identities a process runs under.
///
/// The two variants are the whole vocabulary. There is no shared identity:
/// a development session cannot name itself with a release identity, and an
/// activated release cannot name itself with a development identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionIdentity {
    /// `dev:<app_id>:<workspace_id>:<dev_session_id>` — a mutable workspace.
    Dev {
        app_id: String,
        workspace_id: String,
        dev_session_id: String,
    },
    /// `release:<app_uuid>:<release_id>:<activation_epoch>` — an activated
    /// release directory. `activation_epoch` is the Unix second the
    /// `current` pointer moved onto this release, so restarting the same
    /// release under a new activation is a distinguishable identity.
    Release {
        app_uuid: String,
        release_id: String,
        activation_epoch: u64,
    },
}

impl ExecutionIdentity {
    /// The release id this identity runs, if it is a release identity. A
    /// development identity has none — that is the point.
    #[must_use]
    pub fn release_id(&self) -> Option<&str> {
        match self {
            Self::Dev { .. } => None,
            Self::Release { release_id, .. } => Some(release_id),
        }
    }

    /// Parse an identity string back into its parts. Returns `None` for any
    /// string that is not one of the two shapes.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let (kind, rest) = raw.split_once(':')?;
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            return None;
        }
        match kind {
            "dev" => Some(Self::Dev {
                app_id: parts[0].to_string(),
                workspace_id: parts[1].to_string(),
                dev_session_id: parts[2].to_string(),
            }),
            "release" => Some(Self::Release {
                app_uuid: parts[0].to_string(),
                release_id: parts[1].to_string(),
                activation_epoch: parts[2].parse().ok()?,
            }),
            _ => None,
        }
    }
}

impl std::fmt::Display for ExecutionIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dev {
                app_id,
                workspace_id,
                dev_session_id,
            } => write!(f, "dev:{app_id}:{workspace_id}:{dev_session_id}"),
            Self::Release {
                app_uuid,
                release_id,
                activation_epoch,
            } => write!(f, "release:{app_uuid}:{release_id}:{activation_epoch}"),
        }
    }
}

// ─── Capability grant ─────────────────────────────────────────────────────

/// The capability scope an execution identity may hold.
///
/// The grant is *derived* from the identity, never configured next to it.
/// That is what keeps the third separation level structural: there is no
/// call that hands a development session a release grant, because
/// [`CapabilityScope::of`] is a total function from the identity and it has
/// no branch that crosses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum CapabilityScope {
    /// Reaches one mutable development workspace and nothing else.
    Workspace {
        app_id: String,
        workspace_id: String,
        workspace: String,
    },
    /// Reaches one release directory and nothing else.
    Release {
        app_uuid: String,
        release_id: String,
        version_dir: String,
    },
}

impl CapabilityScope {
    /// The grant for `identity`. A development identity gets a workspace
    /// grant rooted at `place`; a release identity gets a release grant
    /// rooted at its version directory.
    #[must_use]
    pub fn of(identity: &ExecutionIdentity, place: &Path) -> Self {
        match identity {
            ExecutionIdentity::Dev {
                app_id,
                workspace_id,
                ..
            } => Self::Workspace {
                app_id: app_id.clone(),
                workspace_id: workspace_id.clone(),
                workspace: place.display().to_string(),
            },
            ExecutionIdentity::Release {
                app_uuid,
                release_id,
                ..
            } => Self::Release {
                app_uuid: app_uuid.clone(),
                release_id: release_id.clone(),
                version_dir: place.display().to_string(),
            },
        }
    }

    /// The single directory this grant reaches. Anything outside it is
    /// outside the grant.
    #[must_use]
    pub fn root(&self) -> &str {
        match self {
            Self::Workspace { workspace, .. } => workspace,
            Self::Release { version_dir, .. } => version_dir,
        }
    }

    /// Does this grant cover `path`? The check is on the resolved path, so
    /// a symlink or a `..` segment cannot walk out of the grant.
    #[must_use]
    pub fn covers(&self, path: &Path) -> bool {
        let root = std::fs::canonicalize(self.root());
        let target = std::fs::canonicalize(path);
        match (root, target) {
            (Ok(root), Ok(target)) => target.starts_with(&root),
            // An unresolvable path is outside every grant. Refusing is the
            // safe answer; a caller that meant a new file resolves its
            // parent instead.
            _ => false,
        }
    }
}

// ─── Status ───────────────────────────────────────────────────────────────

/// The status vocabulary. `Dev` and `Unmanaged` belong to development;
/// every other value belongs to a published app.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AppStatus {
    /// A development session in a mutable workspace.
    Dev,
    /// A development workspace Host Track does not manage.
    Unmanaged,
    /// Desired = installed = active = observed, and the probe is green.
    Current,
    /// A published app whose four release ids do not all agree, or whose
    /// probe is not green. Carries why, so an operator does not have to
    /// re-derive it.
    Drift(String),
    /// A published app whose observation is missing or older than
    /// [`OBSERVATION_STALE_AFTER`]. `CURRENT` is a live claim, so a stale
    /// observation is not enough to make it.
    Unknown(String),
}

impl AppStatus {
    /// The one-word label an operator reads.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Dev => "DEV",
            Self::Unmanaged => "UNMANAGED",
            Self::Current => "CURRENT",
            Self::Drift(_) => "DRIFT",
            Self::Unknown(_) => "UNKNOWN",
        }
    }

    #[must_use]
    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current)
    }
}

/// The status of a development session.
///
/// This function is the whole development status path, and its return type
/// makes rule 1 structural: it produces `Dev` when Host Track manages the
/// workspace and `Unmanaged` when it does not. There is no branch that
/// yields `Current`, so no development session can claim it.
#[must_use]
pub fn dev_status(identity: &ExecutionIdentity, workspace: &Path) -> AppStatus {
    // Managed means both halves: a development identity AND a workspace
    // that is really there. A release identity is not a development
    // session, so it falls to `Unmanaged` — which keeps the caller out of
    // `CURRENT` without pretending the workspace exists.
    let managed = matches!(identity, ExecutionIdentity::Dev { .. }) && workspace.is_dir();
    if managed {
        AppStatus::Dev
    } else {
        AppStatus::Unmanaged
    }
}
