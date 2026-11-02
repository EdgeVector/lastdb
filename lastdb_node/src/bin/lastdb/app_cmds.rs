use super::*;

#[path = "app_cmds/arg_groups.rs"]
mod arg_groups;
#[path = "app_cmds/managed.rs"]
mod managed;
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
#[path = "app_cmds/interface_publish.rs"]
mod interface_publish;
use interface_publish::{publish_interface, InterfacePublish};
#[path = "app_cmds/public_catalog.rs"]
mod public_catalog;
use public_catalog::{print_public_list, public_list};
#[path = "app_cmds/registry.rs"]
mod registry;
pub(crate) use self::registry::*;
#[path = "app_cmds/local_install.rs"]
mod local_install;
pub(crate) use self::local_install::*;
/// Print `value` as pretty JSON on stdout (an empty string if it cannot be
/// serialized, as every `--json` path here always did).
pub(crate) fn print_pretty_json<T: serde::Serialize + ?Sized>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

pub(crate) fn crypto_env_for(env: Option<&str>) -> Result<app_identity_crypto::Env, String> {
    match env {
        Some("dev") | None => Ok(app_identity_crypto::Env::Dev),
        Some("prod") => Ok(app_identity_crypto::Env::Prod),
        Some(other) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
    }
}

pub(crate) fn block_on_app<T>(
    fut: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    runtime.block_on(fut)
}

/// `lastdb app ...`: resolve the home, then hand each subcommand to its
/// handler in `check`, `publish`, `registry` or `release`. Release and dev
/// status subcommands go through [`app_release_command`].
pub(super) fn app_command(data_dir: Option<PathBuf>, action: AppCommand) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        AppCommand::Managed(command) => managed::command(&home, command),
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
            registry_url,
            channel,
        }) => {
            if schema_url.is_some() || api_url.is_some() {
                return Err("publish uses the separate app registry; use --registry-url".into());
            }
            publish_interface(
                &home,
                InterfacePublish {
                    manifest,
                    environment: env,
                    registry_url,
                    channel,
                    key_file,
                    api_key,
                },
            )
        }
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
        accept_install,
        inactive,
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
            accept_install,
            inactive,
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
        all,
        accept_install,
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
            all,
            accept_install,
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
        } => app_info(home, &app_id, env, schema_url, channel, index, trust_key),
        command @ AppCatalogCommand::Install { .. } => handle_app_install(home, command),
        command @ AppCatalogCommand::Upgrade { .. } => handle_app_upgrade(home, command),
    }
}
