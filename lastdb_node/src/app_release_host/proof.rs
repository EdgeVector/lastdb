use super::*;

// ─── The four-way proof ───────────────────────────────────────────────────

/// The four release ids plus the probe — the whole status contract for a
/// published app, in one printable value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FourWayProof {
    /// From `GET /v2/apps/{app_id}/channels/{channel}`.
    pub desired: Option<String>,
    /// The verified version directory on disk.
    pub installed: Option<String>,
    /// What `current` resolves to.
    pub active: Option<String>,
    /// What the live process reports.
    pub observed: Option<String>,
    /// Every distinct release id reported by a live process. More than one
    /// means an old release process is still running.
    pub observed_all: Vec<String>,
    pub probe: ProbeOutcome,
    pub status: AppStatus,
    /// The one-word status, flat, so a reader does not have to know how the
    /// [`AppStatus`] variants serialize. `DEV`, `UNMANAGED`, `CURRENT`,
    /// `DRIFT`, or `UNKNOWN`.
    pub status_label: String,
}

impl FourWayProof {
    /// Apply status rule 2. `Current` requires four equal ids, exactly one
    /// distinct live observation, and a green probe. Any inequality, a
    /// missing or stale observation, or a red probe removes `Current`.
    #[must_use]
    pub fn evaluate(
        desired: Option<String>,
        installed: Option<String>,
        active: Option<String>,
        observed_all: Vec<String>,
        probe: ProbeOutcome,
    ) -> Self {
        let observed = (observed_all.len() == 1).then(|| observed_all[0].clone());
        let status = match (&desired, &installed, &active) {
            (Some(d), Some(i), Some(a)) if d == i && i == a => {
                if observed_all.is_empty() {
                    AppStatus::Unknown(
                        "no live process observation is fresh enough to prove CURRENT".to_string(),
                    )
                } else if observed_all.len() > 1 {
                    AppStatus::Drift(format!(
                        "{} live processes report different release ids: {}",
                        observed_all.len(),
                        observed_all.join(", ")
                    ))
                } else if observed_all[0] != *a {
                    AppStatus::Drift(format!(
                        "the live process observes {}, the active release is {a}",
                        observed_all[0]
                    ))
                } else if let ProbeOutcome::Red(reason) = &probe {
                    AppStatus::Drift(format!("probe is not green: {reason}"))
                } else {
                    AppStatus::Current
                }
            }
            // A desired id the host could not read is an ABSENT reading, not
            // a mismatch. The channel read fails on any registry blip, and
            // calling that drift would roll a healthy host back every time
            // the network hiccups. Report it as unknown and change nothing.
            (None, _, _) => AppStatus::Unknown(
                "the channel read did not answer, so DESIRED is unknown".to_string(),
            ),
            _ => AppStatus::Drift(format!(
                "release ids differ — desired {}, installed {}, active {}",
                desired.as_deref().unwrap_or("-"),
                installed.as_deref().unwrap_or("-"),
                active.as_deref().unwrap_or("-"),
            )),
        };
        Self {
            desired,
            installed,
            active,
            observed,
            observed_all,
            probe,
            status_label: status.label().to_string(),
            status,
        }
    }

    /// True when all four ids are equal and the probe is green.
    #[must_use]
    pub fn matches(&self) -> bool {
        self.status.is_current()
    }
}

/// Collect the four release ids and evaluate the status.
///
/// `desired` comes from the caller's channel read, so the check can be run
/// against a channel the caller already fetched (and its generation kept).
#[must_use]
pub fn prove_four_way(host: &HostTrack, desired: Option<String>) -> FourWayProof {
    let active = host.active_release_id();
    let installed = active
        .as_deref()
        .and_then(|id| host.installed_release_id(id));
    let observed_all: Vec<String> = host
        .live_observations()
        .into_iter()
        .map(|o| o.release_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let probe = active.as_deref().map_or_else(
        || ProbeOutcome::Red("no active release".to_string()),
        |id| run_probe(host, id),
    );
    FourWayProof::evaluate(desired, installed, active, observed_all, probe)
}
