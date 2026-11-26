//! Host jobs: periodic TTL sweeps and the ops history.

mod erase;
pub mod ops_history;

pub mod ttl_sweep {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use fold_db::clock::unix_secs;
    use fold_db::db_operations::with_query_page_tally;
    use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
    use fold_db::schema::types::key_comparison::KeyComparisonContext;
    use fold_db::schema::types::schema::DeclarativeSchemaType;
    use fold_db::schema::types::{KeyValue, Query};
    use fold_db::schema::SchemaRetentionPolicy;
    use tokio::task::JoinHandle;
    use tracing::Instrument;

    use crate::host::Host;
    use crate::jobs::erase;

    pub(super) const AGE_KEY_DIGITS: usize = 20;

    pub fn spawn_periodic(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
        let interval = Duration::from_secs(
            fold_db::env_flag::var_or("LASTDB_TTL_SWEEP_INTERVAL_SECS", 60u64).max(1),
        );
        let enabled = || {
            !std::env::var("LASTDB_TTL_SWEEP")
                .is_ok_and(|v| matches!(v.trim(), "0" | "false" | "no" | "off"))
        };
        tokio::spawn(
            async move {
                if enabled() {
                    erase::warn_on_bad_verb_once();
                }
                while !shutdown.load(Ordering::Acquire) {
                    if enabled() {
                        sweep_ttl(&host, &shutdown).await;
                    }
                    let until = Instant::now() + interval;
                    while !shutdown.load(Ordering::Acquire) && Instant::now() < until {
                        let rem = until.saturating_duration_since(Instant::now());
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
            Err(e) => {
                tracing::warn!(target: "lastdbd::ttl_sweep", error = %e, elapsed_ms = started.elapsed().as_millis() as u64, "ttl sweep failed");
                return;
            }
        };
        let (now, mut backlogs) = (unix_secs(), Vec::new());
        let mut totals = erase::PassTotals {
            schemas: policies.len(),
            ..erase::PassTotals::default()
        };
        for (name, policy) in policies {
            if shutdown.load(Ordering::Acquire) {
                totals.cut = true;
                break;
            }
            match prepare_backlog(host, &name, policy, now).await {
                Ok(listed) => backlogs.extend(listed),
                Err(e) => {
                    tracing::warn!(target: "lastdbd::ttl_sweep", schema = %name, error = %e, "ttl sweep could not list expired keys of schema; skipping for this pass");
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
            tracing::info!(target: "lastdbd::ttl_sweep", removed = totals.erased, dropped_after_recheck = totals.dropped, elapsed_ms, "ttl sweep purged expired retained rows");
        }
        if totals.keys_left > 0 || totals.cut {
            tracing::info!(target: "lastdbd::ttl_sweep", keys_left = totals.keys_left, cut = totals.cut, schemas_failed = totals.schemas_failed, "ttl sweep pass ended with listed keys left over; the next pass lists them again");
        }
    }

    pub(super) struct Backlog {
        pub(super) schema_name: String,
        pub(super) schema_type: DeclarativeSchemaType,
        pub(super) fields: usize,
        pub(super) alternates: usize,
        pub(super) batch_cap: usize,
        pub(super) cutoff: u64,
        pub(super) source: Source,
        pub(super) keys: Vec<KeyValue>,
        pub(super) abandoned: usize,
    }

    pub(super) enum Source {
        Ages(Option<String>),
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
            matches!(self.source, Source::Done) && self.keys.is_empty()
        }

        pub(super) fn abandon(&mut self) {
            self.source = Source::Done;
            self.abandoned += std::mem::take(&mut self.keys).len();
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
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => Source::Ages(None),
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
            fields: schema.fields.as_ref().map_or(1, |f| f.len().max(1)),
            alternates: schema
                .record_keys
                .as_ref()
                .map_or(0, |k| k.alternates.len()),
            batch_cap: usize::MAX,
            cutoff: now.saturating_sub(policy.ttl_seconds),
            source,
            keys: Vec::new(),
            abandoned: 0,
        }))
    }

    pub(super) async fn fill_page(
        host: &Host,
        backlog: &mut Backlog,
        limit: usize,
    ) -> Result<(), String> {
        let mut keys = match &mut backlog.source {
            Source::Done => return Ok(()),
            Source::Ages(after) => {
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
                if next.is_some() {
                    *after = next;
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
                let q = Query::new_with_filter(backlog.schema_name.clone(), Some(filter));
                let (rows, end) =
                    with_query_page_tally(host.db.query_executor().query_window(q, window)).await;
                let page: Vec<KeyValue> = rows.map_err(|e| e.to_string())?.into_keys().collect();
                let resume = end.resume;
                *leading &= page.is_empty();
                if let Some(next) = page
                    .iter()
                    .chain(&resume)
                    .max_by(|a, b| KeyComparisonContext::cmp_page_order(a, b))
                    .cloned()
                {
                    if after
                        .as_ref()
                        .is_some_and(|p| !KeyComparisonContext::cmp_page_order(&next, p).is_gt())
                    {
                        return Err("TTL discovery cursor did not advance".into());
                    }
                    if *leading && resume.is_some() {
                        floors.keep(&backlog.schema_name, hash, &next);
                    }
                    *after = Some(next);
                } else {
                    floors.end(&backlog.schema_name, hash);
                    (*partition, *after) = (*partition + 1, None);
                }
                page.into_iter()
                    .filter(|k| erase::key_is_expired(k, &backlog.schema_type, backlog.cutoff))
                    .collect()
            }
        };
        keys.sort_by(|a, b| a.hash.cmp(&b.hash).then_with(|| a.range.cmp(&b.range)));
        keys.dedup();
        backlog.keys = keys;
        Ok(())
    }
}
