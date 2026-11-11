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
        AdminCommand::Shared(action) => shared(data_dir, action),
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

fn shared(data_dir: Option<PathBuf>, action: AdminSharedCommand) -> Result<(), String> {
    let action = match action {
        AdminSharedCommand::Snapshot(args) => return snapshot_cmd::run(data_dir.as_deref(), args),
        AdminSharedCommand::Restore(args) => CloudCommand::RestoreShared(args),
        AdminSharedCommand::ResumeRestore(args) => CloudCommand::ResumeSharedRestore(args),
        AdminSharedCommand::Writer(args) => CloudCommand::SharedWriter(args),
    };
    cloud_command(data_dir, action)
}

pub(crate) fn db_post_json(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let response = post_json_admin(socket, path, body)?;
    parse_json_response(&response, path)
}

fn schema_retention_command(
    data_dir: Option<PathBuf>,
    action: SchemaRetentionCommand,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = home.join("data").join("folddb.sock");
    let mut confirm = false;
    let (action, schema, ttl_seconds, hash_partitions, json_only) = match action {
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
        SchemaRetentionCommand::Get { schema, json } => ("get", schema, None, Vec::new(), json),
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
            json,
        ),
        SchemaRetentionCommand::Clear {
            schema,
            confirm_expire_existing,
            json,
        } => {
            confirm = confirm_expire_existing;
            ("clear", schema, None, Vec::new(), json)
        }
    };
    let response = db_post_json(
        &socket,
        "/api/db/schema-retention",
        &serde_json::json!({
            "action": action,
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
                .map_err(|error| format!("serialize schema retention response: {error}"))?
        );
        return Ok(());
    }
    let shown_schema = response
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<unknown>");
    match response.get("retention_policy") {
        Some(serde_json::Value::Object(policy)) => {
            let ttl_seconds = policy
                .get("ttl_seconds")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| "response retention_policy missing ttl_seconds".to_string())?;
            let hash_partitions = policy
                .get("hash_partitions")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len);
            let source = policy
                .get("source")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("operator");
            println!(
                "Schema retention for {shown_schema}: {ttl_seconds}s source={source} (hash partitions: {hash_partitions})"
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
            match response.get("anchor").and_then(serde_json::Value::as_str) {
                Some(anchor) if anchor != shown_schema => {
                    println!(
                        "  policy state is held under {anchor}, shared by both installed names"
                    );
                }
                _ => {}
            }
        }
        _ => println!("Schema retention for {shown_schema}: unset"),
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
    let multiplier = match suffix {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => {
            return Err(format!(
                "invalid retention duration {input:?}; use s, m, h, or d"
            ))
        }
    };
    magnitude
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
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
            .ok_or_else(|| "backfill response has no tracked count".to_string())?;
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
            .ok_or_else(|| "incomplete backfill response has no cursor".to_string())?;
        if cursor.as_deref() == Some(next) {
            return Err("retention backfill cursor did not advance".into());
        }
        cursor = Some(next.to_string());
    }
    if json_only {
        println!(
            "{}",
            serde_json::json!({ "schema": schema, "scope": "personal",
            "tracked": tracked, "pages": pages, "complete": true })
        );
    } else {
        println!(
            "Retention backfill for {schema}: {tracked} tracked records in {pages} durable pages"
        );
    }
    Ok(())
}
