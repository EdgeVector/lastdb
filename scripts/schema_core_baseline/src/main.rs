//! In-memory snapshot of `schema_service_core` observable outputs.
//!
//! Prints one sorted JSON document on stdout:
//! canonical-field seed hashes, schema identity rows, release rows, and
//! channel generations. Wall-clock fields are removed. Run with the schema
//! configuration variables unset so the process environment cannot move
//! the result.
//!
//! ```text
//! cargo run --locked -p schema_core_baseline --bin schema-core-baseline
//! ```
//!
//! Compare against the commit before `SchemaConfig` with the same sources:
//!
//! ```text
//! RUSTFLAGS="--cfg schema_core_pre_config" \
//!   cargo run --locked -p schema_core_baseline --bin schema-core-baseline
//! ```

mod release;
mod snapshot;
mod state;

use serde_json::Value;

const AMBIENT_ENV: &[&str] = &[
    "APP_IDENTITY_REVOKED_DEV_PUBKEYS",
    "APP_IDENTITY_ROOT_PUBKEYS",
    "ENVIRONMENT",
    "REGISTRY_INDEX_SIGNING_KEY_B64",
    "SCHEMA_COMPOSITIONAL_DECOMPOSITION",
    "SCHEMA_DUAL_SIGNAL_CANONICALIZATION",
    "SCHEMA_MUTATION_GATE_APP_QUOTA",
    "SCHEMA_MUTATION_GATE_BASE_DIFFICULTY_BITS",
    "SCHEMA_MUTATION_GATE_CHALLENGE_TTL_SECS",
    "SCHEMA_MUTATION_GATE_DEV_NOVEL_DAY_QUOTA",
    "SCHEMA_MUTATION_GATE_DEV_NOVEL_HOUR_QUOTA",
    "SCHEMA_MUTATION_GATE_DEV_QUOTA",
    "SCHEMA_MUTATION_GATE_ENFORCE",
    "SCHEMA_MUTATION_GATE_HMAC_SECRET",
    "SCHEMA_MUTATION_GATE_IP_QUOTA",
    "SCHEMA_MUTATION_GATE_MAX_ADAPTIVE_DIFFICULTY_BITS",
    "SCHEMA_MUTATION_GATE_MAX_CATALOG_DIFFICULTY_BITS",
    "SCHEMA_MUTATION_GATE_NODE_NOVEL_DAY_QUOTA",
    "SCHEMA_MUTATION_GATE_NODE_NOVEL_HOUR_QUOTA",
    "SCHEMA_MUTATION_GATE_NODE_QUOTA",
    "SCHEMA_MUTATION_GATE_NOVEL_DAY_WINDOW_SECS",
    "SCHEMA_MUTATION_GATE_NOVEL_HOUR_WINDOW_SECS",
    "SCHEMA_MUTATION_GATE_QUOTA_WINDOW_SECS",
    "SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE",
    "SCHEMA_NATIVE_RESOLVE_BUDGET_MS",
    "SCHEMA_REUSE_BEFORE_NEW",
    "SCHEMA_REUSE_FIELD_FIDELITY_FLOOR",
    "SCHEMA_REUSE_PURPOSE_THRESHOLD",
    "SCHEMA_REUSE_STRUCT_FLOOR",
    "SCHEMA_SHADOW_MODE",
];

fn main() {
    refuse_ambient_env();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime");
    let document = runtime.block_on(snapshot::capture());
    println!("{document}");
}

fn refuse_ambient_env() {
    let set: Vec<&str> = AMBIENT_ENV
        .iter()
        .copied()
        .filter(|name| std::env::var_os(name).is_some())
        .collect();
    if set.is_empty() {
        return;
    }
    eprintln!(
        "refusing to run with schema configuration set in the environment: {}",
        set.join(", ")
    );
    std::process::exit(2);
}

pub(crate) fn fail(context: &str, detail: impl std::fmt::Display) -> ! {
    eprintln!("{context}: {detail}");
    std::process::exit(1);
}

const WALL_CLOCK_FIELDS: &[&str] = &[
    "captured_at",
    "created_at",
    "expires_at",
    "issued_at",
    "published_at",
    "registered_at",
    "revoked_at",
    "updated_at",
    "verified_at",
];

pub(crate) fn normalize(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(sorted_object(map)),
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        other => other,
    }
}

fn sorted_object(map: serde_json::Map<String, Value>) -> serde_json::Map<String, Value> {
    let mut keys: Vec<_> = map.into_iter().collect();
    keys.sort_by(|left, right| left.0.cmp(&right.0));
    let mut out = serde_json::Map::new();
    for (key, child) in keys {
        if WALL_CLOCK_FIELDS.contains(&key.as_str()) || is_secret_field(&key) {
            continue;
        }
        out.insert(key, normalize(child));
    }
    out
}

fn is_secret_field(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.contains("secret") || lower.contains("hmac") || lower.contains("private_key")
}
