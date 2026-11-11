use super::*;

pub(super) fn run(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::ServiceHome(action) => service_home_command(action),
        AdminCommand::Schema(AdminSchemaCommand::Drop(args)) => schema_drop_command(
            data_dir,
            args.schema,
            args.owner_app,
            args.must_exist,
            args.json,
        ),
        AdminCommand::Retention(action) => schema_retention_command(data_dir, action),
        AdminCommand::Storage(action) => db_storage_command(data_dir, action),
        AdminCommand::Backup(action) => cloud_command(data_dir, backup_action(action)?),
    }
}

fn backup_action(action: AdminBackupCommand) -> Result<CloudCommand, String> {
    let action = match action {
        AdminBackupCommand::Init(args) => ByteBackupCommand::Init(args),
        AdminBackupCommand::Resume(args) => ByteBackupCommand::Resume(args),
        AdminBackupCommand::Close(args) => ByteBackupCommand::Close(args),
        AdminBackupCommand::InspectDependencies(args) => {
            ByteBackupCommand::InspectDependencies(args)
        }
        AdminBackupCommand::RecoverCredentialsHome { args, existing_dev } => {
            if !existing_dev {
                return Ok(CloudCommand::RecoverCredentialsHome(args));
            }
            if args.env != "dev" {
                return Err("--existing-dev requires --env dev".into());
            }
            return Ok(CloudCommand::RecoverDevCredentialsHome {
                into: args.into,
                expected_user_hash: args.expected_user_hash,
                identity_seed_stdin: args.identity_seed_stdin,
                json: args.json,
            });
        }
        AdminBackupCommand::PrepareRecoveryHome(args) => {
            return Ok(CloudCommand::PrepareRecoveryHome(args));
        }
    };
    Ok(CloudCommand::BackupV2 { action })
}

pub(crate) fn db_post_json(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    parse_json_response(&post_json_admin(socket, path, body)?, path)
}

fn schema_retention_command(
    data_dir: Option<PathBuf>,
    action: SchemaRetentionCommand,
) -> Result<(), String> {
    let socket = lastdb_node::host::resolve_home(data_dir)?
        .join("data")
        .join("folddb.sock");
    let (verb, schema, ttl_seconds, hash_partitions, confirm, json_only) = match action {
        SchemaRetentionCommand::Backfill {
            schema,
            page_size,
            confirm_expire_existing,
            json,
        } => {
            return run_retention_backfill(
                &socket,
                &schema,
                page_size,
                confirm_expire_existing,
                json,
            );
        }
        SchemaRetentionCommand::Get { schema, json } => {
            ("get", schema, None, Vec::new(), false, json)
        }
        SchemaRetentionCommand::Set {
            schema,
            ttl,
            hash_partitions,
            json,
        } => (
            "set",
            schema,
            Some(parse_retention_duration(&ttl)?),
            hash_partitions,
            false,
            json,
        ),
        SchemaRetentionCommand::Clear {
            schema,
            confirm_expire_existing,
            json,
        } => (
            "clear",
            schema,
            None,
            Vec::new(),
            confirm_expire_existing,
            json,
        ),
    };
    let response = db_post_json(
        &socket,
        "/api/db/schema-retention",
        &serde_json::json!({
            "action": verb,
            "schema": schema,
            "ttl_seconds": ttl_seconds,
            "hash_partitions": hash_partitions,
            "confirm_expire_existing": confirm,
        }),
    )?;
    print_retention_policy(&response, json_only)
}

fn print_retention_policy(response: &serde_json::Value, json_only: bool) -> Result<(), String> {
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(response)
                .map_err(|e| format!("serialize schema retention response: {e}"))?
        );
        return Ok(());
    }
    let shown = response
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<unknown>");
    let Some(policy) = response
        .get("retention_policy")
        .and_then(serde_json::Value::as_object)
    else {
        println!("Schema retention for {shown}: unset");
        return Ok(());
    };
    let ttl_seconds = policy
        .get("ttl_seconds")
        .and_then(serde_json::Value::as_u64)
        .ok_or("response retention_policy missing ttl_seconds")?;
    let hash_partitions = policy
        .get("hash_partitions")
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len);
    let source = policy
        .get("source")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("operator");
    println!(
        "Schema retention for {shown}: {ttl_seconds}s source={source} (hash partitions: {hash_partitions})"
    );
    if let Some(manifest_ttl) = policy
        .get("overridden_manifest_ttl_seconds")
        .and_then(serde_json::Value::as_u64)
    {
        println!(
            "  the operator policy overrides a manifest policy of {manifest_ttl}s; `clear` puts it back"
        );
    }
    if response
        .get("existing_records_may_expire")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        println!(
            "  WARNING: existing_records_may_expire: the shorter manifest TTL is in force; records older than {ttl_seconds}s are deleted for good"
        );
    }
    if let Some(anchor) = response
        .get("anchor")
        .and_then(serde_json::Value::as_str)
        .filter(|a| *a != shown)
    {
        println!("  policy state is held under {anchor}, shared by both installed names");
    }
    Ok(())
}

fn parse_retention_duration(input: &str) -> Result<u64, String> {
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, suffix) = input.split_at(split);
    if number.is_empty() || suffix.is_empty() || suffix.len() > 1 {
        return Err(format!(
            "invalid retention duration {input:?}; use a positive integer followed by s, m, h, or d"
        ));
    }
    let magnitude = number.parse::<u64>().map_err(|_| {
        format!("invalid retention duration {input:?}; duration must fit in u64 seconds")
    })?;
    let mult = match suffix {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => {
            return Err(format!(
                "invalid retention duration {input:?}; use s, m, h, or d"
            ))
        }
    };
    magnitude
        .checked_mul(mult)
        .filter(|s| *s > 0)
        .ok_or_else(|| {
            format!("invalid retention duration {input:?}; duration must be greater than zero")
        })
}

fn run_retention_backfill(
    socket: &Path,
    schema: &str,
    page_size: u16,
    confirmed: bool,
    json_only: bool,
) -> Result<(), String> {
    if !confirmed {
        return Err("backfill can expire existing records; pass --confirm-expire-existing".into());
    }
    let mut cursor: Option<String> = None;
    let (mut tracked, mut pages) = (0u64, 0u64);
    loop {
        let response = db_post_json(
            socket,
            "/api/db/schema-retention",
            &serde_json::json!({
                "action": "backfill", "schema": schema, "limit": page_size,
                "cursor": cursor, "confirm_expire_existing": true,
            }),
        )?;
        tracked += response
            .get("tracked")
            .and_then(serde_json::Value::as_u64)
            .ok_or("backfill response has no tracked count")?;
        pages += 1;
        if response
            .get("complete")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            break;
        }
        let next = response
            .get("next_cursor")
            .and_then(serde_json::Value::as_str)
            .ok_or("incomplete backfill response has no cursor")?;
        if cursor.as_deref() == Some(next) {
            return Err("retention backfill cursor did not advance".into());
        }
        cursor = Some(next.to_string());
    }
    if json_only {
        println!(
            "{}",
            serde_json::json!({
                "schema": schema, "scope": "personal",
                "tracked": tracked, "pages": pages, "complete": true
            })
        );
    } else {
        println!(
            "Retention backfill for {schema}: {tracked} tracked records in {pages} durable pages"
        );
    }
    Ok(())
}
