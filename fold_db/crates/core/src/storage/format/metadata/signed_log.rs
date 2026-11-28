//! Opaque rows that the node keeps for its signed logs, such as grants, leases and
//! proposal states.
//!
//! Each row is `signed-log:<key>` in the metadata namespace, beside the `signed-replay-*`
//! rows, so a snapshot carries it. The store does not read the value. A row is staged in
//! the transaction of the write it belongs to: a local commit, or the apply of an entry
//! of another stream.

use super::{read_err, Metadata};
use crate::schema::types::{LogRow, SchemaError};
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_value::MAX_RECORD_BYTES;

type Res<T = ()> = Result<T, SchemaError>;

const PREFIX: &str = "signed-log:";
/// The longest key a caller may give, in bytes.
const MAX_KEY_BYTES: usize = 512;
/// The most rows one page of [`Metadata::signed_log_rows`] returns.
pub const SIGNED_LOG_PAGE_MAX: usize = 1000;

fn row_key(key: &str) -> Res<String> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES || key.chars().any(char::is_control) {
        return Err(SchemaError::InvalidData(format!(
            "a signed-log key is 1 to {MAX_KEY_BYTES} bytes with no control characters"
        )));
    }
    Ok(format!("{PREFIX}{key}"))
}

impl Metadata {
    /// The value of one signed-log row, in the current scope.
    pub(crate) async fn signed_log_row(&self, key: &str) -> Res<Option<Vec<u8>>> {
        self.require_scope().map_err(read_err)?;
        let key = row_key(key)?;
        self.store.get(key.as_bytes()).await.map_err(read_err)
    }

    /// The signed-log rows whose key starts with `prefix`, in key order, after the key
    /// `after` when one is given, at most `limit`. Keys come back without `signed-log:`.
    pub(crate) async fn signed_log_rows(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Res<Vec<(String, Vec<u8>)>> {
        self.require_scope().map_err(read_err)?;
        if limit == 0 || limit > SIGNED_LOG_PAGE_MAX {
            return Err(SchemaError::InvalidData(format!(
                "a page of signed-log rows holds 1 to {SIGNED_LOG_PAGE_MAX} rows"
            )));
        }
        let (lower, upper) = exact_prefix_bounds(&format!("{PREFIX}{prefix}"));
        let start = match after {
            Some(after) => format!("{}\0", row_key(after)?).max(lower),
            None => lower,
        };
        let rows = self
            .store
            .scan_range_paged(start.as_bytes(), upper.as_bytes(), limit)
            .await
            .map_err(read_err)?;
        rows.into_iter()
            .map(|(key, value)| {
                String::from_utf8(key)
                    .ok()
                    .and_then(|key| key.strip_prefix(PREFIX).map(str::to_owned))
                    .map(|key| (key, value))
                    .ok_or_else(|| {
                        SchemaError::InvalidData("a signed-log row has a bad key".into())
                    })
            })
            .collect()
    }

    /// Stage puts and deletes of signed-log rows in the current scope.
    pub(crate) async fn stage_signed_log_rows(&self, rows: &[LogRow]) -> Res {
        let mut staged = Vec::new();
        for row in rows {
            if row
                .value
                .as_ref()
                .is_some_and(|value| value.len() > MAX_RECORD_BYTES)
            {
                return Err(SchemaError::InvalidData(format!(
                    "a signed-log row holds at most {MAX_RECORD_BYTES} bytes"
                )));
            }
            staged.push((row_key(&row.key)?, row.value.clone()));
        }
        if staged.is_empty() {
            return Ok(());
        }
        self.stage(staged).await.map_err(read_err)
    }
}
