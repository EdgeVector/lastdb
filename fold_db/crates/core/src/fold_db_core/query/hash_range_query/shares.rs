use super::HashRangeQueryProcessor;
use crate::schema::types::field::{FieldKind, FieldValue, FieldVariant, HashRangeFilter};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

impl HashRangeQueryProcessor {
    /// Look up active `ShareSubscription`s and return a deduped list of
    /// `from:{sender_hash}` namespace prefixes to scan. Returns an empty
    /// vector if the store has no subscriptions.
    ///
    /// Subscriptions with an unparseable `share_prefix` are logged and
    /// skipped (never silently). Inactive subscriptions are excluded.
    #[cfg(feature = "sharing")]
    pub(super) async fn collect_received_from_namespaces(&self) -> Vec<String> {
        let subs = match crate::sharing::store::list_share_subscriptions_in_ops(&self.db_ops).await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "HashRangeQueryProcessor: failed to list share subscriptions: {}",
                    e
                );
                return Vec::new();
            }
        };

        let mut namespaces: Vec<String> = Vec::new();
        for sub in subs {
            if !sub.active {
                continue;
            }
            match Self::parse_sender_hash(&sub.share_prefix) {
                Some(sender_hash) => {
                    let ns = format!("from:{sender_hash}");
                    if !namespaces.contains(&ns) {
                        namespaces.push(ns);
                    }
                }
                None => {
                    // Redacted, and this one matters more than the query logs:
                    // `error!` is the level that reaches Sentry, so the raw
                    // string left the machine. A share prefix is
                    // `share:{sender}:{recipient}` — two user identity hashes —
                    // and the case that fires here is precisely the case where
                    // the string is not the shape we assumed.
                    //
                    // The segment count is kept in the clear because that is
                    // the actual diagnostic: "got 2, expected 3" is what tells
                    // an operator whether this is a truncation, an old format,
                    // or something else entirely. The `<id:..>` token still
                    // correlates repeats of the same bad subscription.
                    tracing::error!(
                        "HashRangeQueryProcessor: subscription has unparseable \
                         share_prefix {} with {} colon-separated segment(s) \
                         (expected 3, 'share:{{sender}}:{{recipient}}'); skipping.",
                        observability::redact_id!(&sub.share_prefix),
                        sub.share_prefix.split(':').count()
                    );
                }
            }
        }
        namespaces
    }

    #[cfg(not(feature = "sharing"))]
    #[allow(clippy::unused_async)] // API matches the sharing-enabled async path
    pub(super) async fn collect_received_from_namespaces(&self) -> Vec<String> {
        Vec::new()
    }

    /// Resolve a field's values from a specific storage namespace by
    /// temporarily rewriting the field's `storage_prefix`. The field's
    /// structure is cloned with molecule data stripped so the caller's schema
    /// state is not mutated.
    pub(super) async fn resolve_field_from_namespace(
        &self,
        field: &FieldVariant,
        namespace: &str,
        filter: Option<HashRangeFilter>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        let mut cloned = field.cloned_without_molecule();
        cloned
            .common_mut()
            .set_storage_prefix(Some(namespace.to_string()));
        cloned
            .resolve_value(&self.db_ops, filter, as_of, include_tombstones)
            .await
    }

    pub(super) fn stamp_shared_writer(namespace: &str, shared: &mut HashMap<KeyValue, FieldValue>) {
        // Respect any existing molecule-level writer_pubkey and fall back to
        // the namespace sender hash only when unset.
        let sender_hash = Self::sender_hash_from_namespace(namespace);
        for fv in shared.values_mut() {
            if fv.writer_pubkey.is_none() {
                fv.writer_pubkey = Some(sender_hash.clone());
            }
        }
    }

    pub(super) fn sort_keys_for_field(field: &FieldVariant, keys: &mut [KeyValue]) {
        match field.kind {
            FieldKind::Hash => keys.sort_by(|a, b| {
                a.hash
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.hash.as_deref().unwrap_or(""))
            }),
            FieldKind::Range => keys.sort_by(|a, b| {
                a.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.range.as_deref().unwrap_or(""))
            }),
            FieldKind::HashRange | FieldKind::Single => keys.sort_by(|a, b| {
                a.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.range.as_deref().unwrap_or(""))
                    .then_with(|| {
                        a.hash
                            .as_deref()
                            .unwrap_or("")
                            .cmp(b.hash.as_deref().unwrap_or(""))
                    })
            }),
        }
    }

    /// Parse the sender hash from a share prefix of the form
    /// `share:{sender_hash}:{recipient_hash}`. Returns `None` if the prefix
    /// doesn't match this structure.
    #[cfg(feature = "sharing")]
    fn parse_sender_hash(share_prefix: &str) -> Option<String> {
        let mut parts = share_prefix.split(':');
        let kind = parts.next()?;
        if kind != "share" {
            return None;
        }
        let sender = parts.next()?;
        if sender.is_empty() {
            return None;
        }
        Some(sender.to_string())
    }

    /// Extract the sender hash from a `from:{sender_hash}` namespace prefix.
    fn sender_hash_from_namespace(namespace: &str) -> String {
        namespace
            .strip_prefix("from:")
            .unwrap_or(namespace)
            .to_string()
    }
}
