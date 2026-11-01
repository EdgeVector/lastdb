//! CI-only generated inbound data for real daemon range measurements.
//!
//! This executable uses ordinary mutation/storage APIs. It neither contacts a
//! sender nor measures sync, and cannot run outside a fresh owned CI home.

// The mutation future crosses enough nested types to exceed rustc's default.
#![recursion_limit = "256"]

use fold_db::access::{AccessContext, CallerTransport};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::Query;
use fold_db::schema::types::Mutation;
use fold_db::sharing::types::{ShareRule, ShareScope, ShareSubscription};
use lastdb_node::Host;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const SENDERS: [&str; 2] = [
    "11111111111111111111111111111111",
    "22222222222222222222222222222222",
];
/// The send check shares this generated schema. Its hash is outside every
/// workload selector.
const SEND_SCHEMA: &str = "ReceivedRangeBatchesAlternate";
const SEND_HASH: &str = "send-check";
const SEND_FIELDS: [&str; 4] = ["bucket", "id", "title", "body"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Batch {
    source: Option<usize>,
    mutations: Vec<Mutation>,
}

fn checked_home() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true")
        || std::env::var("LASTDB_ISOLATED_COPY").as_deref() != Ok("1")
        || std::env::var("LASTDB_SUPPRESS_RECOVERY_PHRASE").as_deref() != Ok("1")
    {
        return Err("generated received data requires the isolated GitHub Actions recipe".into());
    }
    let raw = PathBuf::from(std::env::args().nth(1).ok_or("missing generated home")?);
    let home = raw.canonicalize()?;
    let temporary = PathBuf::from(std::env::var("RUNNER_TEMP")?).canonicalize()?;
    let workspace = PathBuf::from(std::env::var("GITHUB_WORKSPACE")?).canonicalize()?;
    if raw != home || !home.starts_with(&temporary) || home.starts_with(workspace) {
        return Err("the generated home must be a real path outside Git under RUNNER_TEMP".into());
    }
    let root = home.parent().ok_or("missing generated home parent")?;
    let owner: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("owner.json"))?)?;
    let metadata = root.symlink_metadata()?;
    if root.parent() != Some(temporary.as_path())
        || metadata.mode() & 0o777 != 0o700
        || owner["root"].as_str() != root.to_str()
        || owner["uid"].as_u64() != Some(u64::from(metadata.uid()))
        || owner["device"].as_u64() != Some(metadata.dev())
        || owner["inode"].as_u64() != Some(metadata.ino())
        || owner["generated_data_only"] != true
        || !home.join(".received-range-ci").is_file()
    {
        return Err("the exact generated home ownership differs".into());
    }
    if lastdb_node::cloud::cloud_credentials_path(&home).exists()
        || lastdb_node::cloud::cloud_resume_required_path(&home).exists()
    {
        return Err("the generated home has cloud configuration".into());
    }
    for socket in ["q.sock", "f.sock"] {
        if std::os::unix::net::UnixStream::connect(root.join(socket)).is_ok() {
            return Err("the generated home still has an active daemon".into());
        }
    }
    Ok(home)
}

async fn populate(host: &Host, input: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    if input.metadata()?.len() > 128 * 1024 * 1024 {
        return Err("the generated mutation input exceeds 128 MiB".into());
    }
    let mut count = 0;
    for (line_number, line) in BufReader::new(std::fs::File::open(input)?)
        .lines()
        .enumerate()
    {
        if line_number >= 1024 {
            return Err("the generated mutation input exceeds 1024 batches".into());
        }
        let line = line?;
        if line.len() > 1024 * 1024 {
            return Err("a generated mutation batch exceeds one MiB".into());
        }
        let batch: Batch = serde_json::from_str(&line)?;
        if batch.mutations.is_empty() || batch.mutations.len() > 128 {
            return Err("the generated mutation batch is empty or exceeds 128 mutations".into());
        }
        let prefix = batch
            .source
            .map(|source| {
                SENDERS
                    .get(source)
                    .map(|sender| format!("from:{sender}"))
                    .ok_or("unknown generated received source")
            })
            .transpose()?;
        let offered = batch.mutations.len();
        count += offered;
        if count > 50000 {
            return Err("the generated mutation input exceeds 50000 mutations".into());
        }
        let accepted = host
            .db
            .mutation_manager()
            .write_mutations_batch_async(batch.mutations, prefix.as_deref())
            .await?;
        if accepted.len() != offered {
            return Err("the generated mutation batch has an incomplete acknowledgement".into());
        }
    }
    activate_generated_receivers(host).await?;
    count += check_generated_send(host).await?;
    host.db
        .db_ops()
        .namespaced_store()
        .restore_durability_barrier()
        .await?;
    Ok(count)
}

async fn activate_generated_receivers(host: &Host) -> Result<(), Box<dyn std::error::Error>> {
    let subscriptions = host
        .db
        .db_ops()
        .open_namespace("share_subscriptions")
        .await?;
    let mut controls = Vec::with_capacity(SENDERS.len());
    for sender in SENDERS {
        let subscription = ShareSubscription {
            sender_pubkey: format!("generated-ci-{sender}"),
            share_prefix: format!("share:{sender}:generated-range-ci"),
            share_e2e_secret: Vec::new(),
            accepted_at: 1,
            active: true,
            share_grant: None,
        };
        controls.push((
            sender.as_bytes().to_vec(),
            serde_json::to_vec(&subscription)?,
        ));
    }
    subscriptions.batch_put(controls).await?;
    Ok(())
}

/// The generated share rule of the send check: one recipient scope for the
/// send schema.
async fn activate_generated_send_rule(host: &Host) -> Result<String, Box<dyn std::error::Error>> {
    let share_prefix = format!("share:{}:generated-send-check", SENDERS[0]);
    let rule = ShareRule {
        rule_id: "generated-send-check".into(),
        recipient_pubkey: format!("generated-ci-{}", SENDERS[0]),
        recipient_display_name: "generated send check".into(),
        scope: ShareScope::Schema(SEND_SCHEMA.into()),
        share_prefix: share_prefix.clone(),
        share_e2e_secret: Vec::new(),
        active: true,
        created_at: 1,
        writer_pubkey: "generated-ci".into(),
        signature: String::new(),
    };
    let rules = host.db.db_ops().open_namespace("share_rules").await?;
    rules
        .batch_put(vec![(
            rule.rule_id.as_bytes().to_vec(),
            serde_json::to_vec(&rule)?,
        )])
        .await?;
    Ok(share_prefix)
}

/// A personal write to a schema with an active share rule also writes each
/// changed record in the rule's scope. Write two records and update one, then
/// read the hash in both scopes; the values must match. Returns the number of
/// mutations.
async fn check_generated_send(host: &Host) -> Result<usize, Box<dyn std::error::Error>> {
    let share_prefix = activate_generated_send_rule(host).await?;
    let batches = [
        vec![
            send_mutation("create", "a", &send_record("a", "first"))?,
            send_mutation("create", "b", &send_record("b", "second"))?,
        ],
        vec![send_mutation(
            "update",
            "b",
            &json!({"title": "second-updated"}),
        )?],
    ];
    let mut count = 0;
    for batch in batches {
        let offered = batch.len();
        let accepted = host
            .db
            .mutation_manager()
            .write_mutations_batch_async(batch, None)
            .await?;
        if accepted.len() != offered {
            return Err("the generated send batch has an incomplete acknowledgement".into());
        }
        count += offered;
    }
    let personal = read_send_hash(host, None).await?;
    let shared = read_send_hash(host, Some(share_prefix)).await?;
    let updated = personal
        .get("title")
        .and_then(|values| values.get("b"))
        .and_then(Value::as_str);
    if personal.values().any(|values| values.len() != 2) || updated != Some("second-updated") {
        return Err("the generated send check misses a personal record".into());
    }
    if shared != personal {
        return Err("the share scope differs from the personal records it copies".into());
    }
    Ok(count)
}

/// One generated personal record of the send check.
fn send_record(range: &str, title: &str) -> Value {
    json!({
        "bucket": SEND_HASH,
        "id": range,
        "title": title,
        "body": format!("{SEND_HASH}-{range}"),
    })
}

/// One generated personal mutation of the send check under the send hash.
fn send_mutation(action: &str, range: &str, fields: &Value) -> Result<Mutation, serde_json::Error> {
    serde_json::from_value(json!({
        "uuid": uuid::Uuid::new_v4().to_string(),
        "schema_name": SEND_SCHEMA,
        "fields_and_values": fields,
        "key_value": {"hash": SEND_HASH, "range": range},
        "pub_key": "generated-ci",
        "mutation_type": action,
        "synchronous": true,
        "source_file_name": null,
    }))
}

/// The values of the send hash in one scope, by field and range.
async fn read_send_hash(
    host: &Host,
    scope: Option<String>,
) -> Result<BTreeMap<String, BTreeMap<String, Value>>, Box<dyn std::error::Error>> {
    let query = Query::new_with_filter(
        SEND_SCHEMA.to_string(),
        SEND_FIELDS.iter().map(ToString::to_string).collect(),
        Some(HashRangeFilter::HashKey(SEND_HASH.to_string())),
    );
    let access = AccessContext::owner(host.user_hash.clone())
        .with_transport(CallerTransport::InProcess)
        .with_storage_prefix(scope);
    let rows = host
        .db
        .query_executor()
        .query_with_access(query, &access)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(field, values)| {
            let values = values
                .into_iter()
                .map(|(key, value)| (key.range.unwrap_or_default(), value.value))
                .collect();
            (field, values)
        })
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = checked_home()?;
    let input =
        PathBuf::from(std::env::args().nth(2).ok_or("missing generated input")?).canonicalize()?;
    if !input.starts_with(PathBuf::from(std::env::var("RUNNER_TEMP")?).canonicalize()?) {
        return Err("the generated input must stay under RUNNER_TEMP".into());
    }
    let host = Host::boot(&home).await.map_err(std::io::Error::other)?;
    let result = populate(&host, &input).await;
    host.db.shutdown().await?;
    println!(
        "{}",
        serde_json::json!({"mutations": result?, "durable": true,
        "received_sources": SENDERS, "generated_data_only": true})
    );
    Ok(())
}
