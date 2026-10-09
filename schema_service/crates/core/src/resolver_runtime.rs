//! Active native resolver state owner: bootstrap, refresh, LKG, kill-switch.
//!
//! Keeps verification/cache invariants in the pack consumer and only swaps
//! `Arc<NativeResolverState>` after a fully verified load. Failed refreshes
//! leave the previous active state intact.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::RwLock;

use crate::resolver_pack_consumer::{
    LoadedResolverPack, ResolverBootstrapConfig, ResolverBootstrapConfigError,
    ResolverPackConsumer, ResolverPackFallbackReason, ResolverPackLoadOutcome,
    ResolverPackLoadSource, ResolverPackObjectStore,
};

/// Immutable, verified pack ready for local resolution.
#[derive(Debug, Clone)]
pub struct NativeResolverState {
    pub pack: LoadedResolverPack,
}

impl NativeResolverState {
    pub fn from_loaded(pack: LoadedResolverPack) -> Self {
        // Config validation already ran during pack verify (`parse_resolver_config`
        // + cross-field checks). No additional product activation here.
        Self { pack }
    }
}

/// Low-cardinality runtime telemetry (no URLs, field names, or proposals).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeTelemetry {
    pub fetch_outcomes: BTreeMap<String, u64>,
    pub verification_outcomes: BTreeMap<String, u64>,
    pub activation_outcomes: BTreeMap<String, u64>,
    pub cache_hits: BTreeMap<String, u64>,
    pub fallback_reasons: BTreeMap<String, u64>,
    pub lkg_loads: BTreeMap<String, u64>,
}

pub struct ResolverRuntime<S: ResolverPackObjectStore> {
    consumer: ResolverPackConsumer<S>,
    active: Arc<RwLock<Option<Arc<NativeResolverState>>>>,
    enabled: AtomicBool,
    latest_etag: Mutex<Option<String>>,
    telemetry: Arc<Mutex<RuntimeTelemetry>>,
}

impl<S> ResolverRuntime<S>
where
    S: ResolverPackObjectStore,
{
    pub fn new(
        store: S,
        bootstrap: &ResolverBootstrapConfig,
    ) -> Result<Self, ResolverBootstrapConfigError> {
        bootstrap.validate_bootstrap()?;
        let consumer =
            ResolverPackConsumer::new(store, bootstrap.fs_cache(), bootstrap.to_consumer_config());
        let enabled = AtomicBool::new(bootstrap.enabled);
        Ok(Self {
            consumer,
            active: Arc::new(RwLock::new(None)),
            enabled,
            latest_etag: Mutex::new(None),
            telemetry: Arc::new(Mutex::new(RuntimeTelemetry::default())),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    pub async fn active(&self) -> Option<Arc<NativeResolverState>> {
        if !self.is_enabled() {
            return None;
        }
        self.active.read().await.clone()
    }

    /// Cold start: try last-known-good first, then one network refresh.
    pub async fn bootstrap_load(&self) -> ResolverPackLoadOutcome {
        if !self.is_enabled() {
            self.record_fallback(ResolverPackFallbackReason::Disabled);
            return ResolverPackLoadOutcome::LiveServiceFallback {
                reason: ResolverPackFallbackReason::Disabled,
            };
        }

        let mut outcome = ResolverPackLoadOutcome::LiveServiceFallback {
            reason: ResolverPackFallbackReason::MissingPack,
        };

        match self.consumer.load_last_known_good_only() {
            Ok((
                ResolverPackLoadOutcome::Loaded {
                    source: ResolverPackLoadSource::LastKnownGood,
                },
                Some(pack),
            )) => {
                self.activate(pack, "lkg").await;
                self.record_lkg("ok");
                outcome = ResolverPackLoadOutcome::Loaded {
                    source: ResolverPackLoadSource::LastKnownGood,
                };
            }
            Ok((ResolverPackLoadOutcome::LiveServiceFallback { reason }, None)) => {
                self.record_lkg(reason.as_str());
                outcome = ResolverPackLoadOutcome::LiveServiceFallback { reason };
            }
            Ok(_) => {}
            Err(_) => {
                self.record_lkg("error");
            }
        }

        // Network refresh once; success upgrades active state. Failure keeps
        // whatever LKG (if any) was activated above.
        let refresh_outcome = self.refresh().await;
        match refresh_outcome {
            ResolverPackLoadOutcome::Loaded {
                source: ResolverPackLoadSource::Latest | ResolverPackLoadSource::NotModified,
            } => refresh_outcome,
            other => {
                if matches!(
                    outcome,
                    ResolverPackLoadOutcome::Loaded {
                        source: ResolverPackLoadSource::LastKnownGood
                    }
                ) {
                    outcome
                } else {
                    other
                }
            }
        }
    }

    /// Network refresh. On failure, previous `active` Arc is left intact.
    pub async fn refresh(&self) -> ResolverPackLoadOutcome {
        if !self.is_enabled() {
            self.record_fallback(ResolverPackFallbackReason::Disabled);
            return ResolverPackLoadOutcome::LiveServiceFallback {
                reason: ResolverPackFallbackReason::Disabled,
            };
        }

        let if_none_match = self.latest_etag.lock().ok().and_then(|guard| guard.clone());

        let result = self
            .consumer
            .load_latest_conditional(if_none_match.as_deref())
            .await;

        match result {
            Ok((
                outcome @ ResolverPackLoadOutcome::Loaded {
                    source: ResolverPackLoadSource::Latest,
                },
                Some(pack),
                etag,
            )) => {
                self.activate(pack, "latest").await;
                if let Ok(mut guard) = self.latest_etag.lock() {
                    *guard = etag;
                }
                outcome
            }
            Ok((
                outcome @ ResolverPackLoadOutcome::Loaded {
                    source: ResolverPackLoadSource::NotModified,
                },
                Some(pack),
                etag,
            )) => {
                // Only activate if we have no active state yet.
                let mut guard = self.active.write().await;
                if guard.is_none() {
                    *guard = Some(Arc::new(NativeResolverState::from_loaded(pack)));
                    self.record_activation("not_modified_cold");
                } else {
                    self.record_cache_hit("latest_not_modified");
                    self.record_activation("not_modified_keep");
                }
                drop(guard);
                if let Ok(mut etag_guard) = self.latest_etag.lock() {
                    *etag_guard = etag;
                }
                outcome
            }
            Ok((
                outcome @ ResolverPackLoadOutcome::Loaded {
                    source: ResolverPackLoadSource::LastKnownGood,
                },
                Some(pack),
                _,
            )) => {
                // Failed latest with valid LKG: only fill empty active; never
                // overwrite a previously activated state on a failed refresh.
                let mut guard = self.active.write().await;
                if guard.is_none() {
                    *guard = Some(Arc::new(NativeResolverState::from_loaded(pack)));
                    self.record_activation("lkg_after_failed_latest");
                    self.record_lkg("activated");
                } else {
                    self.record_activation("keep_previous_after_failed_latest");
                    self.record_lkg("available_not_swapped");
                }
                drop(guard);
                outcome
            }
            Ok((outcome @ ResolverPackLoadOutcome::LiveServiceFallback { reason }, None, _)) => {
                self.record_fallback(reason);
                self.record_activation("fallback_keep_previous");
                outcome
            }
            Ok((outcome, _, _)) => {
                self.record_activation("unexpected_outcome");
                outcome
            }
            Err(_) => {
                self.record_activation("error_keep_previous");
                self.record_fallback(ResolverPackFallbackReason::ArtifactFetchFailed);
                ResolverPackLoadOutcome::LiveServiceFallback {
                    reason: ResolverPackFallbackReason::ArtifactFetchFailed,
                }
            }
        }
    }

    async fn activate(&self, pack: LoadedResolverPack, source: &'static str) {
        let state = Arc::new(NativeResolverState::from_loaded(pack));
        let mut guard = self.active.write().await;
        *guard = Some(state);
        self.record_activation(source);
    }

    fn record_activation(&self, outcome: &'static str) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot
                .activation_outcomes
                .entry(outcome.to_string())
                .or_insert(0) += 1;
        }
    }

    fn record_cache_hit(&self, key: &'static str) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot.cache_hits.entry(key.to_string()).or_insert(0) += 1;
        }
    }

    fn record_fallback(&self, reason: ResolverPackFallbackReason) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot
                .fallback_reasons
                .entry(reason.as_str().to_string())
                .or_insert(0) += 1;
        }
    }

    fn record_lkg(&self, outcome: &str) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot.lkg_loads.entry(outcome.to_string()).or_insert(0) += 1;
        }
    }
}
