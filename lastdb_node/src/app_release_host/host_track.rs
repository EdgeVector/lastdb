use super::*;

// ─── Host Track layout ────────────────────────────────────────────────────

/// The Host Track directory tree for one app.
///
/// ```text
/// <root>/apps/<app_id>/
///   versions/<release-id>/     unpacked, verified release bytes
///     lastdb-release.json      the receipt: release id + manifest + digest
///   current -> versions/<release-id>
///   prior-verified             the last release id that proved CURRENT
///   observed/<pid>.json        one live process's observation
/// ```
#[derive(Debug, Clone)]
pub struct HostTrack {
    root: PathBuf,
    app_id: String,
}

/// One release process's self-report: which release it is actually running.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub release_id: String,
    pub execution_identity: String,
    pub pid: u32,
    /// RFC 3339. Compared against [`OBSERVATION_STALE_AFTER`].
    pub observed_at: String,
}

/// The receipt written into a version directory at unpack time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseReceipt {
    pub release_id: String,
    pub manifest: ReleaseManifest,
    /// The digest the host computed over the bytes it actually downloaded.
    pub verified_artifact_digest: String,
    pub installed_at: String,
}

impl HostTrack {
    /// Root defaults to `~/.host-track`; `LASTDB_HOST_TRACK_ROOT` overrides
    /// it so a test or an ephemeral proof run never touches the real tree.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, app_id: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            app_id: app_id.into(),
        }
    }

    /// The default root: `$LASTDB_HOST_TRACK_ROOT`, else `~/.host-track`.
    #[must_use]
    pub fn default_root() -> PathBuf {
        std::env::var_os("LASTDB_HOST_TRACK_ROOT").map_or_else(
            || {
                std::env::var_os("HOME").map_or_else(
                    || PathBuf::from(".host-track"),
                    |home| PathBuf::from(home).join(".host-track"),
                )
            },
            PathBuf::from,
        )
    }

    #[must_use]
    pub fn app_dir(&self) -> PathBuf {
        self.root.join("apps").join(&self.app_id)
    }

    #[must_use]
    pub fn versions_dir(&self) -> PathBuf {
        self.app_dir().join("versions")
    }

    #[must_use]
    pub fn version_dir(&self, release_id: &str) -> PathBuf {
        self.versions_dir().join(release_id)
    }

    #[must_use]
    pub fn current_link(&self) -> PathBuf {
        self.app_dir().join("current")
    }

    #[must_use]
    pub fn prior_verified_path(&self) -> PathBuf {
        self.app_dir().join("prior-verified")
    }

    #[must_use]
    pub fn observed_dir(&self) -> PathBuf {
        self.app_dir().join("observed")
    }

    /// The **installed** release id: the release the version directory holds
    /// and whose receipt still matches the bytes on disk.
    #[must_use]
    pub fn installed_release_id(&self, release_id: &str) -> Option<String> {
        let receipt = self.read_receipt(release_id)?;
        (receipt.release_id == release_id
            && receipt.verified_artifact_digest == receipt.manifest.artifact_digest)
            .then(|| receipt.release_id.clone())
    }

    /// The receipt inside one version directory.
    #[must_use]
    pub fn read_receipt(&self, release_id: &str) -> Option<ReleaseReceipt> {
        let path = self.version_dir(release_id).join("lastdb-release.json");
        serde_json::from_slice(&std::fs::read(path).ok()?).ok()
    }

    /// The **active** release id: what the `current` pointer resolves to.
    #[must_use]
    pub fn active_release_id(&self) -> Option<String> {
        let target = std::fs::read_link(self.current_link()).ok()?;
        target
            .file_name()
            .and_then(|n| n.to_str())
            .map(str::to_string)
    }

    /// The release id the host last proved CURRENT, and can roll back to.
    #[must_use]
    pub fn prior_verified_release_id(&self) -> Option<String> {
        std::fs::read_to_string(self.prior_verified_path())
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| is_sha256_hex(s))
    }

    /// Record a release id as the rollback target. Called only after the
    /// four-way match and a green probe — a release the host never proved
    /// is not a rollback target.
    ///
    /// # Errors
    /// Returns the filesystem error.
    pub fn set_prior_verified(&self, release_id: &str) -> Result<(), String> {
        std::fs::create_dir_all(self.app_dir())
            .map_err(|e| format!("failed to create {}: {e}", self.app_dir().display()))?;
        std::fs::write(self.prior_verified_path(), release_id)
            .map_err(|e| format!("failed to write prior-verified: {e}"))
    }

    /// Write this process's observation. A release process calls this on
    /// start and on every heartbeat; the host reads the whole set.
    ///
    /// # Errors
    /// Returns the filesystem or serialization error.
    pub fn write_observation(&self, identity: &ExecutionIdentity) -> Result<(), String> {
        let release_id = identity
            .release_id()
            .ok_or_else(|| "only a release identity can be observed".to_string())?;
        let dir = self.observed_dir();
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("failed to create {}: {e}", dir.display()))?;
        let pid = std::process::id();
        let observation = Observation {
            release_id: release_id.to_string(),
            execution_identity: identity.to_string(),
            pid,
            observed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        };
        let body = serde_json::to_vec_pretty(&observation)
            .map_err(|e| format!("failed to encode observation: {e}"))?;
        std::fs::write(dir.join(format!("{pid}.json")), body)
            .map_err(|e| format!("failed to write observation: {e}"))
    }

    /// Every observation from a process that is still alive and whose
    /// report is fresher than [`OBSERVATION_STALE_AFTER`].
    ///
    /// A dead process's file is ignored (and removed) rather than trusted:
    /// an observation is a claim about a live process. A live but silent
    /// process leaves a stale file, which drops the status out of `CURRENT`
    /// instead of letting a cached value stand in for a live one.
    #[must_use]
    pub fn live_observations(&self) -> Vec<Observation> {
        let Ok(entries) = std::fs::read_dir(self.observed_dir()) else {
            return Vec::new();
        };
        let now = chrono::Utc::now();
        let mut live = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(observation) = serde_json::from_slice::<Observation>(&bytes) else {
                continue;
            };
            if !process_is_alive(observation.pid) {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            let fresh =
                chrono::DateTime::parse_from_rfc3339(&observation.observed_at).is_ok_and(|at| {
                    let age = now.signed_duration_since(at.with_timezone(&chrono::Utc));
                    age.num_seconds() >= 0
                        && u64::try_from(age.num_seconds())
                            .is_ok_and(|s| s <= OBSERVATION_STALE_AFTER.as_secs())
                });
            if fresh {
                live.push(observation);
            }
        }
        live
    }

    /// Move the `current` pointer onto a release directory.
    ///
    /// Written as a temporary symlink and renamed over the old one, so a
    /// reader either sees the old target or the new one and never an
    /// absent pointer.
    ///
    /// # Errors
    /// Returns the filesystem error, or a refusal when the target version
    /// directory does not exist — the pointer never names bytes that are
    /// not on disk.
    pub fn activate(&self, release_id: &str) -> Result<u64, String> {
        let target = self.version_dir(release_id);
        if !target.is_dir() {
            return Err(format!(
                "refusing to activate {release_id}: {} is not a directory",
                target.display()
            ));
        }
        let link = self.current_link();
        let staging = self.app_dir().join(".current.staging");
        let _ = std::fs::remove_file(&staging);
        std::os::unix::fs::symlink(PathBuf::from("versions").join(release_id), &staging)
            .map_err(|e| format!("failed to stage current pointer: {e}"))?;
        std::fs::rename(&staging, &link)
            .map_err(|e| format!("failed to move current pointer: {e}"))?;
        Ok(unix_secs())
    }

    /// Delete every version directory except the newest
    /// [`RELEASE_RETENTION`], never deleting the active release or the
    /// prior verified one.
    ///
    /// # Errors
    /// Returns the filesystem error.
    pub fn prune_versions(&self) -> Result<Vec<String>, String> {
        let keep_active = self.active_release_id();
        let keep_prior = self.prior_verified_release_id();
        let Ok(entries) = std::fs::read_dir(self.versions_dir()) else {
            return Ok(Vec::new());
        };
        let mut versions: Vec<(std::time::SystemTime, String)> = Vec::new();
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            // Only release-id directories are versions. A leftover
            // `.staging-*` tree from an interrupted unpack is not a
            // release, and pruning must neither count nor keep it.
            if !is_sha256_hex(&name) {
                let _ = std::fs::remove_dir_all(entry.path());
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH);
            versions.push((modified, name));
        }
        // Newest first, so the tail past the retention bound is the oldest.
        versions.sort_by_key(|(modified, _)| std::cmp::Reverse(*modified));
        let mut removed = Vec::new();
        for (index, (_, release_id)) in versions.iter().enumerate() {
            let protected = keep_active.as_deref() == Some(release_id.as_str())
                || keep_prior.as_deref() == Some(release_id.as_str());
            if index < RELEASE_RETENTION || protected {
                continue;
            }
            std::fs::remove_dir_all(self.version_dir(release_id))
                .map_err(|e| format!("failed to remove {release_id}: {e}"))?;
            removed.push(release_id.clone());
        }
        Ok(removed)
    }
}

/// Is `pid` a live process? `kill(pid, 0)` answers without signalling.
///
/// Only a strictly positive pid is asked about. `kill(0, …)` and
/// `kill(-1, …)` mean "every process in my group" and "every process I may
/// signal", so they succeed regardless of what is running — an observation
/// file naming pid 0 would otherwise read as permanently alive.
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    if pid < 1 {
        return false;
    }
    // SAFETY: `kill` with signal 0 performs the permission and existence
    // check only; it delivers no signal and touches no memory.
    unsafe { libc::kill(pid, 0) == 0 }
}
