use super::super::FoldDB;
use super::inbound_targets;
use std::sync::Arc;

impl FoldDB {
    /// Register an org cloud target for one explicit local database prefix.
    pub async fn register_org_sync_target_for_storage_prefix(
        &self,
        org_hash: &str,
        storage_prefix: &str,
        e2e_key_b64: &str,
        slug: &str,
    ) -> Result<crate::sharing::OrgSyncTarget, crate::error::FoldDbError> {
        let target = crate::sharing::upsert_org_sync_target_for_storage_prefix_in_ops(
            self.db_ops(),
            org_hash,
            storage_prefix,
            e2e_key_b64,
            slug,
        )
        .await?;
        self.apply_org_sync_targets_from_store().await?;
        Ok(target)
    }

    /// Register one schema from the legacy unprefixed personal instance.
    pub async fn register_org_sync_target_for_storage_prefix_and_schema(
        &self,
        org_hash: &str,
        storage_prefix: &str,
        schema_name: &str,
        e2e_key_b64: &str,
        slug: &str,
    ) -> Result<crate::sharing::OrgSyncTarget, crate::error::FoldDbError> {
        let target = crate::sharing::upsert_org_sync_target_for_storage_prefix_and_schema_in_ops(
            self.db_ops(),
            org_hash,
            storage_prefix,
            schema_name,
            e2e_key_b64,
            slug,
        )
        .await?;
        self.apply_org_sync_targets_from_store().await?;
        Ok(target)
    }

    /// Deactivate the active org sync targets that match `org_hash` and/or
    /// `slug`, then rebuild the sync engine target set so the next cycle no
    /// longer pulls from or pushes to them. Local only: no cloud registry
    /// call. Re-arm with `register_org_sync_target*` for the same org.
    pub async fn deactivate_org_sync_targets(
        &self,
        org_hash: Option<&str>,
        slug: Option<&str>,
        dry_run: bool,
    ) -> Result<Vec<crate::sharing::OrgSyncTarget>, crate::error::FoldDbError> {
        let changed = crate::sharing::deactivate_org_sync_targets_matching_in_ops(
            self.db_ops(),
            org_hash,
            slug,
            dry_run,
        )
        .await?;
        if !dry_run && !changed.is_empty() {
            self.apply_org_sync_targets_from_store().await?;
        }
        Ok(changed)
    }

    /// List registered org cloud-sync targets.
    pub async fn list_org_sync_targets(
        &self,
    ) -> Result<Vec<crate::sharing::OrgSyncTarget>, crate::error::FoldDbError> {
        crate::sharing::list_org_sync_targets_in_ops(self.db_ops()).await
    }

    /// After a catalog share, route the source instance to org heads that
    /// already track the target locator prefix.
    pub async fn attach_shared_instance_to_org_targets(
        &self,
        target_db_locator: &str,
        schema_name: &str,
        instance_id: Option<&str>,
    ) -> Result<(), crate::error::FoldDbError> {
        let instance_id = instance_id.unwrap_or(crate::db_operations::UNPREFIXED_INSTANCE_ID);
        let parsed = crate::access::parse_db_locator(target_db_locator)
            .map_err(crate::error::FoldDbError::Config)?;
        let Some(target_prefix) = crate::access::storage_prefix_for(&parsed) else {
            return Ok(());
        };
        let targets = crate::sharing::list_org_sync_targets_in_ops(self.db_ops()).await?;
        for target in targets.into_iter().filter(|t| t.active) {
            if !target.storage_prefixes.contains(&target_prefix)
                && !target.storage_prefixes.iter().any(|p| p == instance_id)
            {
                continue;
            }
            self.register_org_sync_target_for_storage_prefix_and_schema(
                &target.org_hash,
                instance_id,
                schema_name,
                &target.e2e_key_b64,
                &target.slug,
            )
            .await?;
        }
        Ok(())
    }

    /// Rebuild SyncEngine extra targets from the org sync registry (+ active share rules).
    ///
    /// No-op when sync is not configured. Safe to call after every register.
    pub async fn apply_org_sync_targets_from_store(&self) -> Result<(), crate::error::FoldDbError> {
        let Some(engine) = self.sync_engine() else {
            return Ok(());
        };
        let org_targets =
            crate::sharing::list_active_org_sync_targets_in_ops(self.db_ops()).await?;
        let share_rules = crate::sharing::store::list_share_rules_in_ops(self.db_ops())
            .await
            .unwrap_or_default();
        let partitioner = crate::sync::SyncPartitioner::new_with_orgs(&share_rules, &org_targets);

        let mut extra = Vec::new();
        let mut restore_scopes = std::collections::HashMap::new();
        for org in &org_targets {
            let key = crate::sharing::e2e_key_bytes(org)?;
            restore_scopes.insert(
                org.org_hash.clone(),
                if org.storage_prefixes.is_empty() {
                    vec![org.org_hash.clone()]
                } else {
                    org.storage_prefixes.clone()
                },
            );
            extra.push(crate::sync::SyncTarget {
                label: if org.slug.is_empty() {
                    format!("org:{}", &org.org_hash[..8.min(org.org_hash.len())])
                } else {
                    format!("org:{}", org.slug)
                },
                prefix: org.org_hash.clone(),
                crypto: Arc::new(crate::crypto::LocalCryptoProvider::from_key(key)),
                read_binding: None,
            });
        }
        // Outbound share rules as upload targets (existing cross-user shares).
        for rule in share_rules.iter().filter(|r| r.active) {
            if rule.share_e2e_secret.len() != 32 {
                continue;
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&rule.share_e2e_secret);
            restore_scopes.insert(rule.share_prefix.clone(), vec![rule.share_prefix.clone()]);
            extra.push(crate::sync::SyncTarget {
                label: format!("share:{}", rule.rule_id),
                prefix: rule.share_prefix.clone(),
                crypto: Arc::new(crate::crypto::LocalCryptoProvider::from_key(key)),
                read_binding: None,
            });
        }

        extra.extend(inbound_targets::targets(self).await?);
        engine
            .configure_targets_with_restore_scopes(partitioner, extra, restore_scopes)
            .await;
        Ok(())
    }
}
