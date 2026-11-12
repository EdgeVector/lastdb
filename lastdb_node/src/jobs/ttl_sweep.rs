//! Node-local TTL sweep and expired-key discovery for retained series.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fold_db::clock::unix_secs;
use fold_db::db_operations::{with_query_page_tally, PageResume};
use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::operations::Query;
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::{KeyValue, KeyValuePageOrder};
use fold_db::schema::SchemaRetentionPolicy;
use tracing::Instrument;

use crate::host::Host;

mod erase;

const AGE_KEY_DIGITS: usize = 20;

#[derive(Clone, Copy, Default)]
pub(super) struct EraseShape {
    pub(super) fields: usize,
    pub(super) alternates: usize,
}

fn ttl_sweep_enabled() -> bool {
    !matches!(
        std::env::var("LASTDB_TTL_SWEEP")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

/// Spawn the periodic TTL sweep task. Stops cleanly once `shutdown` is set.
pub fn spawn_periodic(
    host: std::sync::Arc<Host>,
    shutdown: std::sync::Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_secs(
        fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_INTERVAL_SECS", 60u64).max(1),
    );
    tokio::spawn(
        async move {
            if ttl_sweep_enabled() {
                erase::warn_on_bad_verb_once();
            }
            while !shutdown.load(Ordering::Acquire) {
                if ttl_sweep_enabled() {
                    sweep_ttl(&host, &shutdown).await;
                }
                let until = tokio::time::Instant::now() + interval;
                while !shutdown.load(Ordering::Acquire) {
                    let rem = until.saturating_duration_since(tokio::time::Instant::now());
                    if rem.is_zero() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250).min(rem)).await;
                }
            }
        }
        .in_current_span(),
    )
}

async fn sweep_ttl(host: &Host, shutdown: &AtomicBool) {
    let started = Instant::now();
    let policies = match host
        .db
        .db_ops()
        .schemas()
        .list_schema_retention_policies()
        .await
    {
        Ok(p) if p.is_empty() => return,
        Ok(p) => p,
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::ttl_sweep", error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64, "ttl sweep failed"
            );
            return;
        }
    };
    let now = unix_secs();
    let mut totals = erase::PassTotals {
        schemas: policies.len(),
        ..erase::PassTotals::default()
    };
    let mut backlogs = Vec::new();
    for (schema_name, policy) in policies {
        if shutdown.load(Ordering::Acquire) {
            totals.cut = true;
            break;
        }
        match prepare_backlog(host, &schema_name, policy, now).await {
            Ok(listed) => backlogs.extend(listed),
            Err(error) => {
                tracing::warn!(
                    target: "lastdbd::ttl_sweep", schema = %schema_name, error = %error,
                    "ttl sweep could not list expired keys of schema; skipping for this pass"
                );
                totals.schemas_failed += 1;
            }
        }
    }
    if !totals.cut {
        totals = erase::purge_round_robin(host, backlogs, shutdown, totals).await;
    }
    let elapsed_ms = started.elapsed().as_millis() as u64;
    if totals.schemas > 0 && totals.schemas_failed >= totals.schemas {
        tracing::warn!(target: "lastdbd::ttl_sweep", schemas = totals.schemas, elapsed_ms, "ttl sweep failed: no schema succeeded");
    } else if totals.erased > 0 || totals.dropped > 0 {
        tracing::info!(
            target: "lastdbd::ttl_sweep", removed = totals.erased,
            dropped_after_recheck = totals.dropped, elapsed_ms,
            "ttl sweep purged expired retained rows"
        );
    }
    if totals.keys_left > 0 || totals.cut {
        tracing::info!(
            target: "lastdbd::ttl_sweep", keys_left = totals.keys_left,
            cut = totals.cut, schemas_failed = totals.schemas_failed,
            "ttl sweep pass ended with listed keys left over; the next pass lists them again"
        );
    }
}

async fn prepare_backlog(
    host: &Host,
    schema_name: &str,
    policy: SchemaRetentionPolicy,
    now: u64,
) -> Result<Option<Backlog>, String> {
    let Some(schema) = host
        .db
        .schema_manager()
        .get_schema_metadata(schema_name)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let source = match &schema.schema_type {
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => Source::Ages { after: None },
        DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange => {
            let hashes = if schema.schema_type == DeclarativeSchemaType::Range {
                vec![String::new()]
            } else {
                let mut h = policy.hash_partitions;
                h.extend(
                    host.db
                        .db_ops()
                        .schemas()
                        .schema_retention_hash_partitions(schema_name)
                        .await
                        .map_err(|e| e.to_string())?,
                );
                h.sort();
                h.dedup();
                h
            };
            Source::Range {
                hashes,
                partition: 0,
                after: None,
                leading: true,
            }
        }
    };
    Ok(Some(Backlog {
        schema_name: schema_name.to_string(),
        schema_type: schema.schema_type.clone(),
        shape: EraseShape {
            fields: schema.fields.as_ref().map_or(1, |f| f.len().max(1)),
            alternates: schema
                .record_keys
                .as_ref()
                .map_or(0, |k| k.alternates.len()),
        },
        batch_cap: usize::MAX,
        cutoff: now.saturating_sub(policy.ttl_seconds),
        source,
        keys: Vec::new(),
        next: 0,
        failed: false,
        abandoned: 0,
    }))
}

pub(super) fn key_is_expired(
    key: &KeyValue,
    schema_type: &DeclarativeSchemaType,
    cutoff: u64,
) -> bool {
    key.range
        .as_deref()
        .and_then(|r| r.get(..AGE_KEY_DIGITS))
        .is_some_and(|digits| {
            digits.bytes().all(|b| b.is_ascii_digit())
                && digits.parse::<u64>().is_ok_and(|t| t < cutoff)
                && match schema_type {
                    DeclarativeSchemaType::Range => true,
                    DeclarativeSchemaType::HashRange => key.hash.is_some(),
                    DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => false,
                }
        })
}

pub(super) struct Backlog {
    pub(super) schema_name: String,
    pub(super) schema_type: DeclarativeSchemaType,
    pub(super) shape: EraseShape,
    pub(super) batch_cap: usize,
    pub(super) cutoff: u64,
    source: Source,
    keys: Vec<KeyValue>,
    next: usize,
    failed: bool,
    abandoned: usize,
}

enum Source {
    Ages {
        after: Option<String>,
    },
    Range {
        hashes: Vec<String>,
        partition: usize,
        after: Option<KeyValue>,
        leading: bool,
    },
    Done,
}

impl Backlog {
    pub(super) fn is_done(&self) -> bool {
        self.failed || (matches!(self.source, Source::Done) && self.left() == 0)
    }
    pub(super) fn left(&self) -> usize {
        self.abandoned + self.page_left()
    }
    pub(super) fn page_left(&self) -> usize {
        self.keys.len().saturating_sub(self.next)
    }
    pub(super) fn abandon(&mut self) {
        self.failed = true;
        self.abandoned += self.page_left();
        self.release_page();
    }
    pub(super) fn release_page(&mut self) {
        self.keys.clear();
        self.next = 0;
    }
    pub(super) fn take(&mut self, n: usize) -> Vec<KeyValue> {
        let end = self.keys.len().min(self.next.saturating_add(n));
        let chunk = self.keys[self.next..end].to_vec();
        self.next = end;
        chunk
    }
    pub(super) fn shrink_after_limit(&mut self, taken: usize) {
        self.next = self.next.saturating_sub(taken);
        self.batch_cap = (taken / 2).max(1);
    }
    pub(super) fn clear(&mut self) {
        self.next = self.keys.len();
        self.source = Source::Done;
    }
}

pub(super) async fn fill_page(
    host: &Host,
    backlog: &mut Backlog,
    limit: usize,
) -> Result<(), String> {
    let mut keys = match &mut backlog.source {
        Source::Done => return Ok(()),
        Source::Ages { after } => {
            let (keys, next) = host
                .db
                .db_ops()
                .schemas()
                .expired_schema_retention_page(
                    &backlog.schema_name,
                    backlog.cutoff,
                    after.as_deref(),
                    limit,
                )
                .await
                .map_err(|e| e.to_string())?;
            if let Some(next) = next {
                *after = Some(next);
            } else {
                backlog.source = Source::Done;
            }
            keys
        }
        Source::Range {
            hashes,
            partition,
            after,
            leading,
        } => {
            let Some(hash) = hashes.get(*partition) else {
                backlog.source = Source::Done;
                return Ok(());
            };
            let floors = host.db.db_ops().schemas().retention_floors();
            if after.is_none() {
                *after = floors.start(&backlog.schema_name, hash);
                *leading = true;
            }
            let filter = HashRangeFilter::HashRangeRange {
                hash: hash.clone(),
                start: "0".repeat(AGE_KEY_DIGITS),
                end: format!("{:0AGE_KEY_DIGITS$}", backlog.cutoff),
            };
            let window = after.clone().map_or_else(
                || KeyWindow::offset(0, limit),
                |c| KeyWindow::after(c, limit),
            );
            let query = Query::new_with_filter(backlog.schema_name.clone(), Some(filter));
            let (rows, end) =
                with_query_page_tally(host.db.query_executor().query_window(query, window)).await;
            let page_keys: Vec<KeyValue> = rows.map_err(|e| e.to_string())?.into_keys().collect();
            let resume = end.resume.map(|PageResume::Key(k)| k);
            *leading &= page_keys.is_empty();
            if let Some(next) = page_keys
                .iter()
                .chain(resume.as_ref())
                .max_by(|a, b| a.cmp_page_order(b))
                .cloned()
            {
                if after
                    .as_ref()
                    .is_some_and(|p| !next.cmp_page_order(p).is_gt())
                {
                    return Err("TTL discovery cursor did not advance".into());
                }
                if *leading && resume.is_some() {
                    floors.keep(&backlog.schema_name, hash, &next);
                }
                *after = Some(next);
            } else {
                floors.end(&backlog.schema_name, hash);
                *partition += 1;
                *after = None;
            }
            page_keys
                .into_iter()
                .filter(|k| key_is_expired(k, &backlog.schema_type, backlog.cutoff))
                .collect()
        }
    };
    keys.sort_by(|a, b| a.hash.cmp(&b.hash).then_with(|| a.range.cmp(&b.range)));
    keys.dedup();
    backlog.keys = keys;
    backlog.next = 0;
    Ok(())
}
