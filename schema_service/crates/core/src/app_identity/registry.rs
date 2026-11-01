//! Config and registry accessors on the app registry.

use super::*;

/// The public app read both HTTP routers answer.
#[derive(Debug)]
pub enum PublicAppReply {
    /// The app exists. HTTP 200. The value is [`SchemaServiceState::app_public_json`].
    Ready(Value),
    /// No app has that id. HTTP 404. The value is the unknown-app body.
    Missing(Value),
}

impl PublicAppReply {
    /// The HTTP status and body for this read.
    ///
    /// Ready is 200. Missing is 404.
    #[must_use]
    pub fn into_http(self) -> (u16, Value) {
        match self {
            Self::Ready(body) => (200, body),
            Self::Missing(body) => (404, body),
        }
    }
}

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

    /// Replace the app-identity config. Called once at startup by the binaries.
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

    /// Anonymous GET body for one app.
    ///
    /// `revoked` is true when the owner pubkey is on the offline denylist.
    /// The keys match `AppRecord::to_public_json`.
    #[must_use]
    pub fn app_public_json(&self, record: &AppRecord) -> Value {
        let revoked = self
            .app_identity_config()
            .revoked_dev_pubkeys
            .contains(&record.owner_dev_pubkey);
        record.to_public_json(revoked)
    }

    /// Classify [`Self::get_app`] for the public HTTP read.
    ///
    /// `Ready` is the public JSON, including `revoked`. `Missing` is the
    /// unknown-app body. A poisoned lock stays `Missing`, same as [`Self::get_app`].
    #[must_use]
    pub fn public_app_reply(&self, app_id: &str) -> PublicAppReply {
        match self.get_app(app_id) {
            Some(record) => PublicAppReply::Ready(self.app_public_json(&record)),
            None => PublicAppReply::Missing(crate::app_release::unknown_app_json(app_id)),
        }
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
