//! Helper functions for the persistence proof.

use schema_service_core::snapshot::{AppMetadata, AppRecord, AppTier};

pub fn make_app(app_id: &str, owner: &str, name: &str) -> AppRecord {
    AppRecord {
        app_id: app_id.to_string(),
        owner_dev_pubkey: owner.to_string(),
        metadata: AppMetadata {
            display_name: name.to_string(),
            description: "Test app".to_string(),
            homepage_url: "https://example.com".to_string(),
            icon_url: None,
        },
        version: "1.0.0".to_string(),
        registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        tier: AppTier::Sandbox,
        code_signature: None,
        source: None,
        artifact: None,
        uses: Vec::new(),
    }
}
