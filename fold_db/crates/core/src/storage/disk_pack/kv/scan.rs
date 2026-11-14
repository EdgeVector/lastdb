//! Bounded prefix, paged range, and yielding scans on [`PackKvStore`].

use super::{native_routing, parse_json, BatchRef};
use crate::storage::disk_pack::read::{NativeRows, ReadContext};
use crate::storage::disk_pack::transaction::{self, traits::YieldedScan};
use crate::storage::disk_pack::{
    run_blocking, PackKvStore, PackRuntime, StorageError, StorageResult,
};
use serde::de::DeserializeOwned;
use std::sync::Arc;

type KvRows = Vec<(Vec<u8>, Vec<u8>)>;
type ScanRes = StorageResult<KvRows>;
type YieldRes = StorageResult<YieldedScan>;
type JsonRows<T> = StorageResult<Vec<(String, T)>>;

fn decode_json_rows<T: DeserializeOwned>(rows: KvRows) -> JsonRows<T> {
    rows.into_iter()
        .map(|(k, v)| {
            let key = String::from_utf8_lossy(&k).into_owned();
            parse_json(&key, &v).map(|val| (key, val))
        })
        .collect()
}

fn prefix_end(prefix: &str) -> Vec<u8> {
    let bump = |(idx, ch): (usize, char)| {
        let next = u32::from(ch) + 1;
        char::from_u32(if next == 0xd800 { 0xe000 } else { next })
            .map(|c| format!("{}{c}", &prefix[..idx]).into_bytes())
    };
    prefix
        .char_indices()
        .rev()
        .find_map(bump)
        .unwrap_or_else(|| vec![255])
}

fn utf8_non_b64<'a>(
    bytes: &'a [u8],
    utf8_err: &'static str,
    b64_err: &'static str,
) -> StorageResult<&'a str> {
    let text = std::str::from_utf8(bytes).map_err(|_| StorageError::invalid(utf8_err))?;
    (!text.starts_with("b64:"))
        .then_some(text)
        .ok_or_else(|| StorageError::invalid(b64_err))
}

fn decode_rows(rows: NativeRows) -> ScanRes {
    rows.into_iter()
        .map(|(k, b)| Ok((native_routing::decode(&k)?, b)))
        .collect()
}

impl PackKvStore {
    pub(crate) async fn list_keys_with_prefix(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let rows = self.scan_prefix(prefix.as_bytes()).await?;
        Ok(rows
            .into_iter()
            .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
            .collect())
    }

    pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned>(&self, p: &str) -> JsonRows<T> {
        decode_json_rows(self.scan_prefix(p.as_bytes()).await?)
    }

    pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned>(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> JsonRows<T> {
        decode_json_rows(
            self.scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
                .await?,
        )
    }

    pub(crate) async fn scan_prefix(&self, prefix: &[u8]) -> ScanRes {
        let utf8_err = "native metadata prefix requires its UTF-8 key codec";
        let start = utf8_non_b64(
            prefix,
            utf8_err,
            "reserved native ids require exact-key reads",
        )?;
        let err = Some("native prefix result requires a bounded continuation");
        self.bounded_rows(start.as_bytes().to_vec(), prefix_end(start), 10_000, err)
            .await
    }

    pub(crate) async fn scan_range_paged(&self, start: &[u8], end: &[u8], limit: usize) -> ScanRes {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        const RANGE_IDS: &str = "native range requires UTF-8 ids or a metadata limit marker";
        utf8_non_b64(start, "native range requires a UTF-8 start", RANGE_IDS)?;
        if std::str::from_utf8(end.strip_suffix(&[255]).unwrap_or(end)).is_err() {
            return Err(StorageError::invalid(RANGE_IDS));
        }
        self.bounded_rows(start.to_vec(), end.to_vec(), limit, None)
            .await
    }

    async fn run_scan<T: Send + 'static>(
        &self,
        work: impl FnOnce(&PackRuntime, &Arc<ReadContext>, BatchRef<'_>, &str) -> StorageResult<T>
            + Send
            + 'static,
    ) -> StorageResult<T> {
        let rt = Arc::clone(&self.runtime);
        let ctx = ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        let obs = Arc::clone(&ctx);
        obs.remembered(run_blocking(move || work(&rt, &ctx, pending.as_ref(), &col)).await)
    }

    async fn bounded_rows(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        err: Option<&'static str>,
    ) -> ScanRes {
        self.run_scan(move |rt, ctx, pending, col| {
            let (rows, exhausted, _) =
                rt.raw_range(ctx, (col, &start, &end), limit, pending, None)?;
            if let Some(detail) = err.filter(|_| !exhausted) {
                return Err(StorageError::read_limit(detail));
            }
            let mut rows = decode_rows(rows)?;
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows.dedup_by(|a, b| a.0 == b.0);
            if err.is_some() && rows.len() > limit {
                let msg = format!("native complete metadata result exceeds {limit} distinct keys");
                return Err(StorageError::read_limit(msg));
            }
            rows.truncate(limit);
            Ok(rows)
        })
        .await
    }

    pub(crate) async fn scan_range_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_run: usize,
    ) -> YieldRes {
        let fallback = limit == 0
            || start >= end
            || utf8_non_b64(start, "", "").is_err()
            || std::str::from_utf8(end).is_err()
            || transaction::PendingBatch::current(&self.runtime).is_some();
        if fallback {
            let rows = self.scan_range_paged(start, end, limit).await?;
            let resume = rows
                .last()
                .filter(|_| limit > 0 && rows.len() >= limit)
                .map(|(k, _)| k.clone());
            return Ok(YieldedScan { rows, resume });
        }
        let (start, end) = (start.to_vec(), end.to_vec());
        self.run_scan(move |rt, ctx, _, col| {
            let (rows, _, res) =
                rt.raw_range(ctx, (col, &start, &end), limit, None, Some(keys_per_run))?;
            let rows = decode_rows(rows)?;
            let resume = res.map(|k| native_routing::decode(&k)).transpose()?;
            Ok(YieldedScan { rows, resume })
        })
        .await
    }
}
