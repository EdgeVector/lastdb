//! Schema-mutation gate defaults and environment-driven configuration.

use super::*;

// 300s leaves headroom for a legitimate single-thread client under modest
// adaptive/catalog pressure. Difficulty is still clamped to the TTL budget
// (see max_difficulty_bits_for_challenge_ttl) so load spikes cannot issue an
// unsolvable challenge that only fails after the client grinds to expiry.
pub(super) const DEFAULT_CHALLENGE_TTL_SECS: u64 = 300;
pub(super) const DEFAULT_BASE_DIFFICULTY_BITS: u8 = 18;
pub(super) const DEFAULT_QUOTA_WINDOW_SECS: u64 = 60;
pub(super) const DEFAULT_NODE_QUOTA: usize = 30;
pub(super) const DEFAULT_IP_QUOTA: usize = 120;
pub(super) const DEFAULT_APP_QUOTA: usize = 60;
pub(super) const DEFAULT_DEV_QUOTA: usize = 120;
pub(super) const DEFAULT_NOVEL_HOUR_WINDOW_SECS: u64 = 60 * 60;
pub(super) const DEFAULT_NOVEL_DAY_WINDOW_SECS: u64 = 24 * 60 * 60;
pub(super) const DEFAULT_NODE_NOVEL_HOUR_QUOTA: usize = 10;
pub(super) const DEFAULT_NODE_NOVEL_DAY_QUOTA: usize = 100;
pub(super) const DEFAULT_DEV_NOVEL_HOUR_QUOTA: usize = 10;
pub(super) const DEFAULT_DEV_NOVEL_DAY_QUOTA: usize = 100;
pub(super) const DEFAULT_MAX_ADAPTIVE_DIFFICULTY_BITS: u8 = 12;
pub(super) const DEFAULT_MAX_CATALOG_DIFFICULTY_BITS: u8 = 12;
pub(super) const DEFAULT_SCHEMA_MUTATION_GATE_HMAC_SECRET: &[u8] =
    b"schema-mutation-gate-dev-secret";
/// Conservative single-thread client hash rate used when sizing challenges so
/// expected grind work finishes inside the challenge TTL with margin.
pub(super) const CONSERVATIVE_CLIENT_HASHES_PER_SEC: u64 = 250_000;
/// Use at most this fraction of the TTL for *expected* (mean) grind work.
pub(super) const CHALLENGE_SOLVE_BUDGET_TTL_NUM: u64 = 1;
pub(super) const CHALLENGE_SOLVE_BUDGET_TTL_DEN: u64 = 4;

#[derive(Debug, Clone)]
pub struct SchemaMutationGateConfig {
    pub enforce_shared_mutations: bool,
    pub challenge_ttl: Duration,
    pub base_difficulty_bits: u8,
    pub quota_window: Duration,
    pub node_quota: usize,
    pub ip_quota: usize,
    pub app_quota: usize,
    pub dev_quota: usize,
    pub novel_hour_window: Duration,
    pub novel_day_window: Duration,
    pub node_novel_hour_quota: usize,
    pub node_novel_day_quota: usize,
    pub dev_novel_hour_quota: usize,
    pub dev_novel_day_quota: usize,
    pub max_adaptive_difficulty_bits: u8,
    pub max_catalog_difficulty_bits: u8,
    pub hmac_secret: Vec<u8>,
}

impl Default for SchemaMutationGateConfig {
    fn default() -> Self {
        Self {
            enforce_shared_mutations: false,
            challenge_ttl: Duration::from_secs(DEFAULT_CHALLENGE_TTL_SECS),
            base_difficulty_bits: DEFAULT_BASE_DIFFICULTY_BITS,
            quota_window: Duration::from_secs(DEFAULT_QUOTA_WINDOW_SECS),
            node_quota: DEFAULT_NODE_QUOTA,
            ip_quota: DEFAULT_IP_QUOTA,
            app_quota: DEFAULT_APP_QUOTA,
            dev_quota: DEFAULT_DEV_QUOTA,
            novel_hour_window: Duration::from_secs(DEFAULT_NOVEL_HOUR_WINDOW_SECS),
            novel_day_window: Duration::from_secs(DEFAULT_NOVEL_DAY_WINDOW_SECS),
            node_novel_hour_quota: DEFAULT_NODE_NOVEL_HOUR_QUOTA,
            node_novel_day_quota: DEFAULT_NODE_NOVEL_DAY_QUOTA,
            dev_novel_hour_quota: DEFAULT_DEV_NOVEL_HOUR_QUOTA,
            dev_novel_day_quota: DEFAULT_DEV_NOVEL_DAY_QUOTA,
            max_adaptive_difficulty_bits: DEFAULT_MAX_ADAPTIVE_DIFFICULTY_BITS,
            max_catalog_difficulty_bits: DEFAULT_MAX_CATALOG_DIFFICULTY_BITS,
            hmac_secret: DEFAULT_SCHEMA_MUTATION_GATE_HMAC_SECRET.to_vec(),
        }
    }
}

impl SchemaMutationGateConfig {
    pub fn from_env() -> Self {
        Self {
            enforce_shared_mutations: env_bool("SCHEMA_MUTATION_GATE_ENFORCE"),
            challenge_ttl: Duration::from_secs(env_flag::var_or(
                "SCHEMA_MUTATION_GATE_CHALLENGE_TTL_SECS",
                DEFAULT_CHALLENGE_TTL_SECS,
            )),
            base_difficulty_bits: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_BASE_DIFFICULTY_BITS",
                DEFAULT_BASE_DIFFICULTY_BITS,
            ),
            quota_window: Duration::from_secs(env_flag::var_or(
                "SCHEMA_MUTATION_GATE_QUOTA_WINDOW_SECS",
                DEFAULT_QUOTA_WINDOW_SECS,
            )),
            node_quota: env_flag::var_or("SCHEMA_MUTATION_GATE_NODE_QUOTA", DEFAULT_NODE_QUOTA),
            ip_quota: env_flag::var_or("SCHEMA_MUTATION_GATE_IP_QUOTA", DEFAULT_IP_QUOTA),
            app_quota: env_flag::var_or("SCHEMA_MUTATION_GATE_APP_QUOTA", DEFAULT_APP_QUOTA),
            dev_quota: env_flag::var_or("SCHEMA_MUTATION_GATE_DEV_QUOTA", DEFAULT_DEV_QUOTA),
            novel_hour_window: Duration::from_secs(env_flag::var_or(
                "SCHEMA_MUTATION_GATE_NOVEL_HOUR_WINDOW_SECS",
                DEFAULT_NOVEL_HOUR_WINDOW_SECS,
            )),
            novel_day_window: Duration::from_secs(env_flag::var_or(
                "SCHEMA_MUTATION_GATE_NOVEL_DAY_WINDOW_SECS",
                DEFAULT_NOVEL_DAY_WINDOW_SECS,
            )),
            node_novel_hour_quota: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_NODE_NOVEL_HOUR_QUOTA",
                DEFAULT_NODE_NOVEL_HOUR_QUOTA,
            ),
            node_novel_day_quota: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_NODE_NOVEL_DAY_QUOTA",
                DEFAULT_NODE_NOVEL_DAY_QUOTA,
            ),
            dev_novel_hour_quota: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_DEV_NOVEL_HOUR_QUOTA",
                DEFAULT_DEV_NOVEL_HOUR_QUOTA,
            ),
            dev_novel_day_quota: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_DEV_NOVEL_DAY_QUOTA",
                DEFAULT_DEV_NOVEL_DAY_QUOTA,
            ),
            max_adaptive_difficulty_bits: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_MAX_ADAPTIVE_DIFFICULTY_BITS",
                DEFAULT_MAX_ADAPTIVE_DIFFICULTY_BITS,
            ),
            max_catalog_difficulty_bits: env_flag::var_or(
                "SCHEMA_MUTATION_GATE_MAX_CATALOG_DIFFICULTY_BITS",
                DEFAULT_MAX_CATALOG_DIFFICULTY_BITS,
            ),
            hmac_secret: std::env::var("SCHEMA_MUTATION_GATE_HMAC_SECRET")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map_or_else(
                    || DEFAULT_SCHEMA_MUTATION_GATE_HMAC_SECRET.to_vec(),
                    String::into_bytes,
                ),
        }
    }
}

pub(super) fn env_bool(name: &str) -> bool {
    env_flag::var_truthy(name)
}
