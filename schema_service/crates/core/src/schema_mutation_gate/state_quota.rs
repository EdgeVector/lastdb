//! Quota consumption and catalog size accounting on the service state.

use super::*;

impl SchemaServiceState {
    pub(super) fn consume_schema_mutation_quota(
        &self,
        cfg: &SchemaMutationGateConfig,
        node_public_key_hash: &str,
        remote_ip: Option<&str>,
        app_id: Option<&str>,
        dev_pubkey: Option<&str>,
    ) -> Result<(), SchemaMutationGateError> {
        let now = schema_types::clock::unix_secs();
        check_quota_bucket(
            &self.schema_mutation_gate_store,
            "node",
            "minute",
            &format!("node:{node_public_key_hash}"),
            cfg.quota_window,
            cfg.node_quota,
            now,
        )?;
        if let Some(ip) = remote_ip.and_then(normalize_ip_bucket) {
            check_quota_bucket(
                &self.schema_mutation_gate_store,
                "ip",
                "minute",
                &format!("ip:{ip}"),
                cfg.quota_window,
                cfg.ip_quota,
                now,
            )?;
        }
        if let Some(app_id) = app_id.filter(|s| !s.is_empty()) {
            check_quota_bucket(
                &self.schema_mutation_gate_store,
                "app",
                "minute",
                &format!("app:{app_id}"),
                cfg.quota_window,
                cfg.app_quota,
                now,
            )?;
        }
        check_quota_bucket(
            &self.schema_mutation_gate_store,
            "node",
            "hour",
            &format!("node:{node_public_key_hash}:hour"),
            cfg.novel_hour_window,
            cfg.node_novel_hour_quota,
            now,
        )?;
        check_quota_bucket(
            &self.schema_mutation_gate_store,
            "node",
            "day",
            &format!("node:{node_public_key_hash}:day"),
            cfg.novel_day_window,
            cfg.node_novel_day_quota,
            now,
        )?;
        if let Some(dev_pubkey) = dev_pubkey.filter(|s| !s.is_empty()) {
            let dev_hash = schema_types::hex::sha256_hex(dev_pubkey.as_bytes());
            check_quota_bucket(
                &self.schema_mutation_gate_store,
                "dev",
                "minute",
                &format!("dev:{dev_hash}"),
                cfg.quota_window,
                cfg.dev_quota,
                now,
            )?;
            check_quota_bucket(
                &self.schema_mutation_gate_store,
                "dev",
                "hour",
                &format!("dev:{dev_hash}:hour"),
                cfg.novel_hour_window,
                cfg.dev_novel_hour_quota,
                now,
            )?;
            check_quota_bucket(
                &self.schema_mutation_gate_store,
                "dev",
                "day",
                &format!("dev:{dev_hash}:day"),
                cfg.novel_day_window,
                cfg.dev_novel_day_quota,
                now,
            )?;
        }
        Ok(())
    }

    pub(super) fn user_schema_count(&self) -> Result<usize, SchemaMutationGateError> {
        read_lock(&self.schemas, "schemas")
            .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))
            .map(|schemas| {
                schemas
                    .values()
                    .filter(|schema| schema.source == SchemaSource::User)
                    .count()
            })
    }
}
