use super::*;

#[path = "app_cmds/arg_groups.rs"]
mod arg_groups;
pub(crate) use self::arg_groups::*;
#[path = "app_cmds/check.rs"]
mod check;
pub(crate) use self::check::*;
#[path = "app_cmds/check_retention.rs"]
mod check_retention;
pub(crate) use self::check_retention::*;
#[path = "app_cmds/index.rs"]
mod index;
pub(crate) use self::index::*;
#[path = "app_cmds/publish.rs"]
mod publish;
pub(crate) use self::publish::*;
#[path = "app_cmds/registry.rs"]
mod registry;
pub(crate) use self::registry::*;
#[path = "app_cmds/local_install.rs"]
mod local_install;
pub(crate) use self::local_install::*;
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
        AppCommand::Lifecycle(AppLifecycleCommand::DevInit { key_file }) => {
            app_dev_init(&home, key_file)
        }
        AppCommand::Lifecycle(AppLifecycleCommand::Check {
            manifest,
            json,
            sync,
            confirm_expire_existing,
            schema_url: _,
        }) => app_check_command(&home, &manifest, json, sync, confirm_expire_existing),
        AppCommand::Lifecycle(AppLifecycleCommand::RegisterSchemas { manifest, .. }) => {
            app_register_schemas(&home, &manifest)
        }
        AppCommand::Lifecycle(AppLifecycleCommand::Publish {
            manifest,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
        }) => app_publish_command(
            &home, &manifest, env, schema_url, api_url, key_file, api_key,
        ),
        AppCommand::Lifecycle(AppLifecycleCommand::Promote {
            manifest,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
        }) => app_promote(
            &home, &manifest, env, schema_url, api_url, key_file, api_key,
        ),
        AppCommand::Lifecycle(AppLifecycleCommand::Run { app_id, dir, args }) => {
            app_run(&home, app_id, dir, args)
        }
        AppCommand::Catalog(action) => app_catalog_command(&home, action),
        AppCommand::Release(action) => app_release_command(&home, action),
    }
}

fn handle_app_resolve(home: &Path, command: AppCatalogCommand) -> Result<(), String> {
    let AppCatalogCommand::Resolve {
        app_id,
        channel,
        index,
        trust_key,
        lastdb_version,
        json,
    } = command
    else {
        unreachable!("catalog resolve only");
    };
    app_resolve(
        home,
        &AppResolveQuery {
            app_id: &app_id,
            channel: channel.as_deref(),
            index: index.as_deref(),
            trust_key: trust_key.as_deref(),
            lastdb_version: lastdb_version.as_deref(),
            json,
        },
    )
}

fn handle_app_install(home: &Path, command: AppCatalogCommand) -> Result<(), String> {
    let AppCatalogCommand::Install {
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
        from,
        rev,
        dev,
    } = command
    else {
        unreachable!("catalog install only");
    };
    app_install(
        home,
        AppInstall {
            app_ids,
            source: RegistrySource {
                env,
                schema_url,
                channel,
                index,
                trust_key,
                lastdb_version,
            },
            dir,
            allow_sandbox,
            force,
            json,
            from,
            rev,
            dev,
        },
    )
}

fn handle_app_upgrade(home: &Path, command: AppCatalogCommand) -> Result<(), String> {
    let AppCatalogCommand::Upgrade {
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
    } = command
    else {
        unreachable!("catalog upgrade only");
    };
    app_upgrade(
        home,
        AppUpgrade {
            app_ids,
            source: RegistrySource {
                env,
                schema_url,
                channel,
                index,
                trust_key,
                lastdb_version,
            },
            dir,
            allow_sandbox,
            json,
        },
    )
}

/// Catalog subcommands of `lastdb app`: list, resolve, index, info, install
/// and upgrade.
fn app_catalog_command(home: &Path, action: AppCatalogCommand) -> Result<(), String> {
    match action {
        AppCatalogCommand::List {
            env,
            schema_url,
            channel,
            index,
            trust_key,
            json,
        } => app_list(home, env, schema_url, channel, index, trust_key, json),
        command @ AppCatalogCommand::Resolve { .. } => handle_app_resolve(home, command),
        AppCatalogCommand::Index(cmd) => app_index_command(home, cmd),
        AppCatalogCommand::Info {
            app_id,
            env,
            schema_url,
            channel,
            index,
            trust_key,
        } => app_info(&app_id, env, schema_url, channel, index, trust_key),
        command @ AppCatalogCommand::Install { .. } => handle_app_install(home, command),
        command @ AppCatalogCommand::Upgrade { .. } => handle_app_upgrade(home, command),
    }
}
