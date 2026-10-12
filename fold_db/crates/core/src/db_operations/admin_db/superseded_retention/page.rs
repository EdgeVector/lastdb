//! One page of the superseded-version retention scan.

use super::*;

const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// Limits and clock for one retention pass.
pub(super) struct RetainPass<'a> {
    pub(super) max_keys: usize,
    pub(super) max_prunes: usize,
    pub(super) storage_prefix: Option<&'a str>,
    pub(super) cutoff_ns: u64,
    pub(super) dry_run: bool,
    pub(super) started: std::time::Instant,
}

impl AtomStore {
    /// Retain one scanned page of `mk:` rows; returns true when the pass budget is spent.
    pub(super) async fn retain_page(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        head: Option<&str>,
        page_start: &mut String,
        report: &mut SupersededVersionRetentionReport,
        pass: &RetainPass<'_>,
    ) -> Result<bool, SchemaError> {
        for (raw_key, raw_value) in rows {
            let mk_key = String::from_utf8_lossy(&raw_key).into_owned();
            *page_start = mk_key.clone();
            if head == Some(mk_key.as_str()) {
                continue;
            }

            if report.keys_scanned as usize >= pass.max_keys
                || report.tips_truncated as usize >= pass.max_prunes
                || (report.keys_scanned > 0 && pass.started.elapsed() >= PASS_TIME_BUDGET)
            {
                report.more_remaining = true;
                return Ok(true);
            }

            report.keys_scanned += 1;
            report.next_after_key = Some(mk_key.clone());

            self.retain_one_tip(
                report,
                &mk_key,
                &raw_value,
                pass.storage_prefix,
                pass.cutoff_ns,
                pass.dry_run,
            )
            .await?;
        }
        Ok(false)
    }
}
