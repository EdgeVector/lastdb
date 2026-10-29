use serde_json::{json, Value};

use super::AppRecord;

impl AppRecord {
    /// Anonymous `GET /v1/apps/{app_id}` and `GET /v2/apps/{app_id}` body.
    ///
    /// Install reads this body and requires `source` and SemVer `version`.
    /// `revoked` is true when the owner pubkey is on the offline denylist.
    #[must_use]
    pub fn to_public_json(&self, revoked: bool) -> Value {
        json!({
            "app_id": self.app_id,
            "owner_dev_pubkey": self.owner_dev_pubkey,
            "metadata": self.metadata,
            "version": self.version,
            "registered_at": self.registered_at,
            "tier": self.tier,
            "code_signature": self.code_signature,
            "source": self.source,
            "artifact": self.artifact,
            "uses": self.uses,
            "revoked": revoked,
        })
    }
}
