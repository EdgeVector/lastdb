use super::*;

#[path = "app_cmds/check.rs"]
mod check;
pub(crate) use self::check::*;
#[path = "app_cmds/index.rs"]
mod index;
pub(crate) use self::index::*;
#[path = "app_cmds/publish.rs"]
mod publish;
pub(crate) use self::publish::*;
#[path = "app_cmds/registry.rs"]
mod registry;
pub(crate) use self::registry::*;
#[path = "app_cmds/release.rs"]
mod release;
pub(crate) use self::release::*;
#[path = "app_cmds/release_registry.rs"]
mod release_registry;
pub(crate) use self::release_registry::*;

/// Print `value` as pretty JSON on stdout (an empty string if it cannot be
/// serialized, as every `--json` path here always did).
pub(crate) fn print_pretty_json<T: serde::Serialize + ?Sized>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

/// `lastdb app ...`: resolve the home, then hand each subcommand to its
/// handler in `check`, `publish`, `registry` or `release`.
pub(super) fn app_command(data_dir: Option<PathBuf>, action: AppCommand) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        AppCommand::DevInit { key_file } => app_dev_init(&home, key_file),
        AppCommand::Check {
            manifest,
            json,
            sync,
            schema_url: _,
        } => app_check_command(&home, &manifest, json, sync),
        AppCommand::RegisterSchemas { manifest, .. } => app_register_schemas(&home, &manifest),
        AppCommand::Publish {
            manifest,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
        } => app_publish_command(
            &home, &manifest, env, schema_url, api_url, key_file, api_key,
        ),
        AppCommand::Promote {
            manifest,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
        } => app_promote(
            &home, &manifest, env, schema_url, api_url, key_file, api_key,
        ),
        AppCommand::List {
            env,
            schema_url,
            channel,
            index,
            trust_key,
            json,
        } => app_list(&home, env, schema_url, channel, index, trust_key, json),
        AppCommand::Resolve {
            app_id,
            channel,
            index,
            trust_key,
            lastdb_version,
            json,
        } => app_resolve(
            &home,
            &app_id,
            channel.as_deref(),
            index.as_deref(),
            trust_key.as_deref(),
            lastdb_version.as_deref(),
            json,
        ),
        AppCommand::Index(cmd) => app_index_command(&home, cmd),
        AppCommand::Storage {
            json,
            reconcile,
            page_size,
        } => {
            let socket = lastdb_uds::uds::socket_path(&home.join("data"));
            app_storage(&socket, json, reconcile, page_size)
        }
        AppCommand::Info {
            app_id,
            env,
            schema_url,
            channel,
            index,
            trust_key,
        } => app_info(&app_id, env, schema_url, channel, index, trust_key),
        AppCommand::Install {
            app_ids,
            env,
            schema_url,
            channel,
            index,
            trust_key,
            lastdb_version,
            dir,
            allow_sandbox,
            force,
            json,
        } => app_install(
            &home,
            app_ids,
            env,
            schema_url,
            channel,
            index,
            trust_key,
            lastdb_version,
            dir,
            allow_sandbox,
            force,
            json,
        ),
        AppCommand::Upgrade {
            app_ids,
            env,
            schema_url,
            channel,
            index,
            trust_key,
            lastdb_version,
            dir,
            allow_sandbox,
            json,
        } => app_upgrade(
            &home,
            app_ids,
            env,
            schema_url,
            channel,
            index,
            trust_key,
            lastdb_version,
            dir,
            allow_sandbox,
            json,
        ),
        AppCommand::ReleasePublish {
            manifest,
            app_uuid,
            source_commit,
            artifact,
            artifact_url,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        } => app_release_publish(
            &home,
            &manifest,
            &app_uuid,
            &source_commit,
            &artifact,
            &artifact_url,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        ),
        AppCommand::ReleaseChannel {
            app_id,
            channel,
            release_id,
            generation,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        } => app_release_channel(
            &home,
            &app_id,
            &channel,
            &release_id,
            generation,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        ),
        AppCommand::ReleaseRevoke {
            app_id,
            release_id,
            reason,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        } => app_release_revoke(
            &home,
            &app_id,
            &release_id,
            reason.as_deref(),
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        ),
        AppCommand::ReleaseInstall {
            app_id,
            channel,
            env,
            schema_url,
            host_track_root,
            json,
        } => app_release_install(&app_id, &channel, env, schema_url, host_track_root, json),
        AppCommand::ReleaseStatus {
            app_id,
            channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
        } => app_release_status(
            &app_id,
            &channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
        ),
        AppCommand::ReleaseCheck {
            app_id,
            channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
            watch,
            interval_secs,
            cycles,
        } => app_release_check(
            &app_id,
            &channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
            watch,
            interval_secs,
            cycles,
        ),
        AppCommand::DevStatus {
            app_id,
            workspace_id,
            dev_session_id,
            workspace,
            json,
        } => app_dev_status(app_id, workspace_id, dev_session_id, &workspace, json),
        AppCommand::ReleaseObserve {
            app_id,
            app_uuid,
            release_id,
            activation_epoch,
            host_track_root,
            hold_secs,
        } => app_release_observe(
            &app_id,
            app_uuid,
            release_id,
            activation_epoch,
            host_track_root,
            hold_secs,
        ),
        AppCommand::Run { app_id, dir, args } => app_run(&home, app_id, dir, args),
    }
}
