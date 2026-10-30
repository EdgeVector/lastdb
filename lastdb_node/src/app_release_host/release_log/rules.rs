use super::*;

// ─── What the log says ────────────────────────────────────────────────────

impl ReleaseLog {
    /// The latest `pass` or `fail` for `(release_id, contract_version)`.
    #[must_use]
    pub fn verdict(&self, release_id: &str, contract_version: &str) -> Option<&LogEntry> {
        self.index
            .verdicts
            .get(&(release_id.to_string(), contract_version.to_string()))
            .map(|position| &self.entries[*position])
    }

    /// The seq of the first `pass` of a release, over all contract versions.
    #[must_use]
    pub fn first_pass_seq(&self, release_id: &str) -> Option<u64> {
        self.index.first_pass.get(release_id).copied()
    }

    /// The first `revoke` that names the release, its app, or its dev key.
    #[must_use]
    pub fn revocation(
        &self,
        release_id: &str,
        app_id: &str,
        dev_cert_sha256: &str,
    ) -> Option<&LogEntry> {
        self.first_revocation(&[
            (RevokeTarget::Release, release_id),
            (RevokeTarget::App, app_id),
            (RevokeTarget::DevCert, dev_cert_sha256),
        ])
    }

    /// The first `revoke` that names an installed release, its app, or any
    /// dev key a verdict in the log attributes the release to.
    #[must_use]
    pub fn installed_release_revocation(
        &self,
        release_id: &str,
        app_id: &str,
    ) -> Option<&LogEntry> {
        let mut targets = vec![
            (RevokeTarget::Release, release_id),
            (RevokeTarget::App, app_id),
        ];
        if let Some(hashes) = self.index.dev_cert_hashes.get(release_id) {
            targets.extend(
                hashes
                    .iter()
                    .map(|hash| (RevokeTarget::DevCert, hash.as_str())),
            );
        }
        self.first_revocation(&targets)
    }

    fn first_revocation(&self, targets: &[(RevokeTarget, &str)]) -> Option<&LogEntry> {
        targets
            .iter()
            .filter_map(|(kind, target)| {
                self.index
                    .revocations
                    .get(&(*kind, (*target).to_string()))
                    .copied()
            })
            .min()
            .map(|position| &self.entries[position])
    }

    /// Refuse a head older than [`RELEASE_LOG_MAX_HEAD_AGE`] at `now`.
    ///
    /// # Errors
    /// No verified head yet, or a head that is too old.
    pub fn require_fresh_head(&self, now: chrono::DateTime<chrono::Utc>) -> Result<(), String> {
        let head = self
            .head
            .as_ref()
            .ok_or_else(|| "the release log has no verified head yet".to_string())?;
        let age = now.signed_duration_since(head.signed_at()?).num_seconds();
        let max = i64::try_from(RELEASE_LOG_MAX_HEAD_AGE.as_secs()).unwrap_or(i64::MAX);
        if age > max {
            return Err(format!(
                "the release log head was signed at {}, more than {} hours ago; a node \
                 refuses new installs and updates on a stale head (installed apps keep running)",
                head.time,
                max / 3600
            ));
        }
        Ok(())
    }

    /// The whole install rule for one release on one contract version: a
    /// fresh head, a `pass` as the latest verdict, the app id the caller
    /// expects, and no revocation of the release, the app, or the dev key.
    ///
    /// # Errors
    /// Names the rule the release breaks.
    pub fn require_installable(
        &self,
        release_id: &str,
        app_id: &str,
        contract_version: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<InstallVerdict, String> {
        self.require_fresh_head(now)?;
        let entry = self.verdict(release_id, contract_version).ok_or_else(|| {
            format!(
                "the release log has no result for release {release_id} on contract version \
                 {contract_version}; the verifier has not passed it for this node"
            )
        })?;
        let LogEntryKind::Pass(pass) = &entry.kind else {
            return Err(format!(
                "the release log does not pass release {release_id} on contract version \
                 {contract_version} ({})",
                entry.summary()
            ));
        };
        if pass.app_id != app_id {
            return Err(format!(
                "the release log names release {release_id} as app '{}', not '{app_id}'",
                pass.app_id
            ));
        }
        if let Some(revoke) = self.revocation(release_id, app_id, &pass.dev_cert_sha256) {
            return Err(format!(
                "refusing release {release_id}: the release log revokes it ({})",
                revoke.summary()
            ));
        }
        Ok(InstallVerdict {
            pass_seq: entry.seq,
            dev_cert_sha256: pass.dev_cert_sha256.clone(),
            head_seq: self.seq(),
        })
    }
}
