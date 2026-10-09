//! Config and registry accessors on the app registry.

use super::*;

impl SchemaServiceState {
    /// Snapshot the current app-identity config (clone under the read lock
    /// so verification doesn't hold the lock).
    pub fn app_identity_config(&self) -> AppIdentityConfig {
        self.app_identity
            .read()
            .map_or_else(|_| AppIdentityConfig::default(), |c| c.clone())
    }

    /// Deployment env label — the value the register / update outcomes
    /// echo back in the `env` response field so the caller can confirm
    /// which registry (dev or prod) saw the write.
    pub fn deployment_env_label(&self) -> String {
        env_label(self.app_identity_config().deployment_env).to_string()
    }

    /// Replace the app-identity config. Called once at startup by the
    /// binaries; tests use it to inject a test root key.
    pub fn configure_app_identity(&self, config: AppIdentityConfig) {
        if let Ok(mut slot) = self.app_identity.write() {
            *slot = config;
        }
    }

    /// Number of registered apps (gauge source).
    pub fn apps_count(&self) -> usize {
        self.apps.read().map_or(0, |a| a.len())
    }

    /// Single registered app by id, or `None` if the id is not in the
    /// registry. Public lookup-by-known-id source for
    /// `GET /v1/apps/{app_id}` — nodes call this on bootstrap to populate
    /// the local app registry without holding a developer credential.
    /// A poisoned lock surfaces as `None` (matches the rest of the
    /// `*_count` / `list_*` helpers' best-effort posture).
    pub fn get_app(&self, app_id: &str) -> Option<AppRecord> {
        self.apps.read().ok().and_then(|a| a.get(app_id).cloned())
    }

    /// Public app shelf for `GET /v1/apps`.
    ///
    /// Only Live, non-revoked apps appear in the default listing. Sandbox rows
    /// still reserve names and support direct owner workflows, but they are
    /// not browsable until promotion flips them live.
    pub fn list_live_apps(&self) -> Vec<AppRecord> {
        let revoked = self.app_identity_config().revoked_dev_pubkeys;
        let Ok(apps) = self.apps.read() else {
            return Vec::new();
        };
        let mut records: Vec<AppRecord> = apps
            .values()
            .filter(|record| {
                record.tier == AppTier::Live && !revoked.contains(&record.owner_dev_pubkey)
            })
            .cloned()
            .collect();
        records.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        records
    }
}
