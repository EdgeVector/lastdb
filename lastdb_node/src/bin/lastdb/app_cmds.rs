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
#[path = "app_cmds/release_dispatch.rs"]
mod release_dispatch;
use self::release_dispatch::app_release_command;
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
/// handler in `check`, `publish`, `registry` or `release`. Release and dev
/// status subcommands go through [`app_release_command`].
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
        action @ (AppCommand::List { .. }
        | AppCommand::Resolve { .. }
        | AppCommand::Index(_)
        | AppCommand::Storage { .. }
        | AppCommand::Info { .. }
        | AppCommand::Install { .. }
        | AppCommand::Upgrade { .. }) => app_catalog_command(&home, action),
        action @ (AppCommand::ReleasePublish { .. }
        | AppCommand::ReleaseChannel { .. }
        | AppCommand::ReleaseRevoke { .. }
        | AppCommand::ReleaseInstall { .. }
        | AppCommand::ReleaseStatus { .. }
        | AppCommand::ReleaseCheck { .. }
        | AppCommand::DevStatus { .. }
        | AppCommand::ReleaseObserve { .. }) => app_release_command(&home, action),
        AppCommand::Run { app_id, dir, args } => app_run(&home, app_id, dir, args),
    }
}

/// Catalog subcommands of `lastdb app`: list, resolve, index, storage, info,
/// install and upgrade.
fn app_catalog_command(home: &Path, action: AppCommand) -> Result<(), String> {
    match action {
        AppCommand::List {
            env,
            schema_url,
            channel,
            index,
            trust_key,
            json,
        } => app_list(home, env, schema_url, channel, index, trust_key, json),
        AppCommand::Resolve {
            app_id,
            channel,
            index,
            trust_key,
            lastdb_version,
            json,
        } => app_resolve(
            home,
            &app_id,
            channel.as_deref(),
            index.as_deref(),
            trust_key.as_deref(),
            lastdb_version.as_deref(),
            json,
        ),
        AppCommand::Index(cmd) => app_index_command(home, cmd),
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
            home,
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
            home,
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
        _ => unreachable!("app_command routes only catalog subcommands here"),
    }
}
