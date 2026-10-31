use serde_json::{json, Map, Value};

use super::AppRecord;

impl AppRecord {
    /// Register, update, and promote replies share these fields.
    ///
    /// `extra` adds the keys for that one reply. `None` and an empty `uses`
    /// stay in the object. This is not [`Self::to_public_json`]: that body
    /// always includes `tier` and `revoked`.
    #[must_use]
    pub fn mutation_json(&self, extra: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
        let mut map = Map::new();
        let mut put = |key: &str, value: Value| {
            map.insert(key.to_owned(), value);
        };
        put("app_id", json!(self.app_id));
        put("owner_dev_pubkey", json!(self.owner_dev_pubkey));
        put("registered_at", json!(self.registered_at));
        put("metadata", json!(self.metadata));
        put("version", json!(self.version));
        put("code_signature", json!(self.code_signature));
        put("source", json!(self.source));
        put("artifact", json!(self.artifact));
        put("uses", json!(self.uses));
        for (key, value) in extra {
            put(key, value);
        }
        Value::Object(map)
    }

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
