//! Selected-key atom history commands over the owner socket.
use super::*;

#[derive(Args, Debug)]
pub(super) struct HistoryArgs {
    schema: String,
    field: String,
    #[arg(long)]
    key_hash: Option<String>,
    #[arg(long)]
    key_range: Option<String>,
    #[arg(long)]
    database: Option<String>,
    #[command(subcommand)]
    action: HistoryAction,
}

#[derive(Subcommand, Debug)]
enum HistoryAction {
    /// Read or change the history policy of this canonical field.
    Policy {
        mode: Option<String>,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// List one bounded page and its continuation cursor.
    List {
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 32)]
        limit: usize,
    },
    /// Fetch one retained value.
    Get { id: String },
    /// Restore one value only if the selected current write is unchanged.
    Restore {
        id: String,
        #[arg(long, required_unless_present = "absent", conflicts_with = "absent")]
        expected_mutation: Option<String>,
        #[arg(long)]
        absent: bool,
    },
    /// Remove one bounded history page; repeat with the returned cursor.
    Purge {
        #[arg(long)]
        cursor: Option<String>,
    },
}

pub(super) fn run(data_dir: Option<PathBuf>, args: HistoryArgs) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let action = match args.action {
        HistoryAction::Policy { mode, seconds } => {
            let policy = match mode.as_deref() {
                None if seconds.is_none() => serde_json::Value::Null,
                Some("off") if seconds.is_none() => serde_json::json!({"mode": "off"}),
                Some("keep") if seconds.is_none() => serde_json::json!({"mode": "keep"}),
                Some("ttl") if seconds.is_some_and(|s| s > 0) => {
                    serde_json::json!({"mode": "ttl", "seconds": seconds})
                }
                _ => return Err("use policy [off|keep|ttl --seconds N]".into()),
            };
            serde_json::json!({"op": "policy", "policy": policy})
        }
        HistoryAction::List { cursor, limit } => {
            serde_json::json!({"op": "list", "cursor": cursor, "limit": limit})
        }
        HistoryAction::Get { id } => serde_json::json!({"op": "get", "id": id}),
        HistoryAction::Restore {
            id,
            expected_mutation,
            absent,
        } => {
            serde_json::json!({"op": "restore", "id": id, "expected_mutation": expected_mutation, "absent": absent})
        }
        HistoryAction::Purge { cursor } => serde_json::json!({"op": "purge", "cursor": cursor}),
    };
    let body = serde_json::json!({
        "location": {"schema": args.schema, "field": args.field,
            "key": {"hash": args.key_hash, "range": args.key_range}},
        "database": args.database, "action": action,
    });
    let result = db_post_json(&socket, "/api/atom-history", &body)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).map_err(|e| e.to_string())?
    );
    Ok(())
}
