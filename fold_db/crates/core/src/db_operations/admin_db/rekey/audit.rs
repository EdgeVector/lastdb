//! Read-only classification of tips that resolved to no atom body.

use super::*;

impl AtomStore {
    /// Classify a page's tips that resolved to no atom body. Reads only — a
    /// mis-derived body is reported, not moved.
    ///
    /// Two batched reads for the whole page: the locators, then (only for the
    /// locators that disagree with the tip-derived partition) whether the body is
    /// simply somewhere else.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(in crate::db_operations::admin_db) async fn rekey_audit_page(
        &self,
        page: &[RekeySlot],
        unresolved: &[usize],
        storage_prefix: Option<&str>,
        cap: usize,
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<(), SchemaError> {
        use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding};

        if unresolved.is_empty() {
            return Ok(());
        }
        // The locator is the only address in the store that does not come from
        // the tip key, so it is the only way to tell a purged atom from one
        // written under a partition this pass never derives.
        let locator_keys: Vec<String> = unresolved
            .iter()
            .map(|&i| page[i].locator_key.clone())
            .collect();
        let locator_rows = self
            .raw()
            .get_items::<Value>(&locator_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("rekey audit get locator: {e}")))?;

        let mut elsewhere_pos: Vec<usize> = Vec::new();
        let mut elsewhere_keys: Vec<String> = Vec::new();
        for (pos, raw) in locator_rows.iter().enumerate() {
            let Some(located) = raw.as_ref().and_then(atom_locator_codec::decode_value) else {
                continue;
            };
            let slot = unresolved[pos];
            if located.as_str() == page[slot].partition.as_str() {
                continue;
            }
            elsewhere_pos.push(pos);
            elsewhere_keys.push(build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(
                    AtomKeyEncoding::PartitionPrefix,
                    Some(&located),
                    &page[slot].uuid,
                ),
            ));
        }
        let elsewhere_present = self
            .rekey_exists(&elsewhere_keys, "rekey audit exists located body")
            .await?;
        let mut body_elsewhere = vec![false; unresolved.len()];
        for (pos, present) in elsewhere_pos.into_iter().zip(elsewhere_present) {
            body_elsewhere[pos] = present;
        }

        for (pos, &slot) in unresolved.iter().enumerate() {
            let located = locator_rows[pos]
                .as_ref()
                .and_then(atom_locator_codec::decode_value);
            let (class, locator_str) = match (&locator_rows[pos], located) {
                (None, _) => (UnresolvedTipClass::NoBodyAnywhere, None),
                (Some(_), None) => (UnresolvedTipClass::UndecodableLocator, None),
                (Some(_), Some(located)) => {
                    let located_str = located.as_str().to_string();
                    // A locator agreeing with the tip while the body is absent is
                    // an orphan — the page's prefixed probe already established
                    // absence, so no second read is needed for that case.
                    let class = if located.as_str() != page[slot].partition.as_str()
                        && body_elsewhere[pos]
                    {
                        UnresolvedTipClass::MisDerivedPartition
                    } else {
                        UnresolvedTipClass::OrphanLocator
                    };
                    (class, Some(located_str))
                }
            };

            match class {
                UnresolvedTipClass::MisDerivedPartition => report.unresolved_mis_derived += 1,
                UnresolvedTipClass::OrphanLocator => report.unresolved_orphan_locator += 1,
                UnresolvedTipClass::UndecodableLocator => {
                    report.unresolved_undecodable_locator += 1;
                }
                UnresolvedTipClass::NoBodyAnywhere => report.unresolved_no_body += 1,
            }

            // Counters classify every unresolved tip; only the detail rows are
            // capped.
            if report.unresolved.len() < cap {
                report.unresolved.push(UnresolvedTip {
                    tip_key: page[slot].tip_key.clone(),
                    atom_uuid: page[slot].uuid.clone(),
                    derived_partition: page[slot].partition.as_str().to_string(),
                    molecule_uuid: page[slot]
                        .partition
                        .molecule_uuid()
                        .unwrap_or_default()
                        .to_string(),
                    schema: None,
                    locator_partition: locator_str,
                    class,
                });
            } else {
                report.unresolved_truncated = true;
            }
        }
        Ok(())
    }
}
