use super::*;
use lastdb_node::{app_publish, app_registry_index as registry};

#[path = "app_cmds/advice.rs"]
mod advice;
#[path = "app_cmds/check.rs"]
mod check;
#[path = "app_cmds/interface_publish.rs"]
mod interface_publish;
#[path = "app_cmds/managed.rs"]
mod managed;

mod publish_rule {
    pub(super) use super::check::{listed_schemas, require_held_schemas_listed};
}

pub(crate) use self::advice::*;
pub(crate) use self::check::*;
use interface_publish::{publish_interface, InterfacePublish};

#[derive(Clone)]
pub(crate) struct RegistrySource {
    pub(crate) channel: Option<String>,
    pub(crate) index: Option<String>,
    pub(crate) trust_key: Option<String>,
}

pub(crate) struct AppInstall {
    pub(crate) accept_install: bool,
    pub(crate) inactive: bool,
    pub(crate) app_ids: Vec<String>,
    pub(crate) source: RegistrySource,
}

pub(crate) struct AppUpgrade {
    pub(crate) all: bool,
    pub(crate) accept_install: bool,
    pub(crate) app_ids: Vec<String>,
    pub(crate) source: RegistrySource,
}

pub(crate) fn print_pretty_json<T: serde::Serialize + ?Sized>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

pub(crate) fn block_on_app<T>(
    fut: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?
        .block_on(fut)
}

pub(crate) fn resolve_key_file(home: &Path, key_file: Option<PathBuf>) -> PathBuf {
    key_file.unwrap_or_else(|| app_publish::default_dev_key_path(home))
}

pub(crate) fn public_list(
    home: &Path,
    channel: Option<&str>,
    index: Option<&str>,
    trust_key: Option<&str>,
) -> Result<registry::InterfaceList, String> {
    let channel = channel.unwrap_or(registry::DEFAULT_CHANNEL);
    let location = registry::IndexLocation::resolve(index)?;
    let (trust, source) = registry::resolve_list_trust_key(trust_key)?;
    warn_trust_override(&source);
    let socket = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    let (_, _, versions) = registry::running_node(&socket);
    let mut list = block_on_app(registry::fetch_list(
        &location,
        channel,
        versions.api_version,
        &trust,
    ))?;
    list.apps.retain(|entry| {
        entry
            .outbox_format_version
            .is_none_or(|v| v == versions.outbox_format_version)
    });
    Ok(list)
}

fn app_resolve(
    home: &Path,
    app_id: &str,
    channel: Option<&str>,
    index: Option<&str>,
    trust_key: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let socket = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    let resolution = resolve_by_proof(&socket, app_id, channel, index, trust_key)?;
    if json {
        if let Some(entry) = &resolution.list_entry {
            print_pretty_json(entry);
        } else {
            print_pretty_json(&resolution);
        }
    } else {
        println!(
            "{} {} @ {} proved with lastdb {} ({}) by {} at {}",
            resolution.app_id,
            resolution.app_version,
            resolution.sha,
            resolution.lastdb_version,
            resolution.lastdb_version_source,
            resolution.proof_run,
            resolution.proved_at
        );
        println!("  source: {}", resolution.source);
        println!(
            "  index: {} (trust: {})",
            resolution.index, resolution.trust
        );
    }
    Ok(())
}

pub(super) fn app_command(data_dir: Option<PathBuf>, action: AppCommand) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        AppCommand::Managed(command) => managed::command(&home, command),
        AppCommand::Lifecycle(AppLifecycleCommand::DevInit { key_file }) => {
            let path = resolve_key_file(&home, key_file);
            let pubkey = app_publish::generate_dev_key(&path)?;
            println!("dev signing key written to {}", path.display());
            println!("dev_pubkey: {pubkey}");
            Ok(())
        }
        AppCommand::Lifecycle(AppLifecycleCommand::Check {
            manifest,
            json,
            sync,
            confirm_expire_existing,
            schema_url,
        }) => app_check_command(
            &home,
            &manifest,
            json,
            sync,
            confirm_expire_existing,
            schema_url,
        ),
        AppCommand::Lifecycle(AppLifecycleCommand::Publish {
            manifest,
            env,
            key_file,
            api_key,
            registry_url,
            channel,
            prepare,
        }) => publish_interface(
            &home,
            InterfacePublish {
                manifest,
                environment: env,
                registry_url,
                channel,
                key_file,
                api_key,
                prepare,
            },
        ),
        AppCommand::Catalog(action) => app_catalog_command(&home, action),
    }
}

fn app_list_command(
    home: &Path,
    app_id: Option<&str>,
    catalog_entry: bool,
    source: &RegistrySource,
    json: bool,
) -> Result<(), String> {
    let (ch, idx, tk) = (
        source.channel.as_deref(),
        source.index.as_deref(),
        source.trust_key.as_deref(),
    );
    match app_id {
        Some(app_id) if catalog_entry => {
            let list = public_list(home, ch, idx, tk)?;
            let entry = list
                .apps
                .iter()
                .find(|e| e.app_id == app_id)
                .ok_or_else(|| {
                    format!(
                        "no app '{app_id}' was proved with lastdb API {}",
                        list.api_version
                    )
                })?;
            print_pretty_json(entry);
            Ok(())
        }
        Some(app_id) => app_resolve(home, app_id, ch, idx, tk, json),
        None => {
            let list = public_list(home, ch, idx, tk)?;
            if json {
                print_pretty_json(&list);
            } else {
                println!(
                    "# {} API {} · sequence {} · generated {}",
                    list.channel, list.api_version, list.seq, list.generated_at
                );
                for entry in &list.apps {
                    println!("{}\t{}\t{}", entry.app_id, entry.app_version, entry.sha);
                }
            }
            Ok(())
        }
    }
}

fn app_catalog_command(home: &Path, action: AppCatalogCommand) -> Result<(), String> {
    match action {
        AppCatalogCommand::List {
            app_id,
            catalog_entry,
            channel,
            index,
            trust_key,
            json,
        } => app_list_command(
            home,
            app_id.as_deref(),
            catalog_entry,
            &RegistrySource {
                channel,
                index,
                trust_key,
            },
            json,
        ),
        AppCatalogCommand::Install {
            accept_install,
            inactive,
            app_ids,
            channel,
            index,
            trust_key,
            json: _,
        } => managed::install(
            home,
            &AppInstall {
                accept_install,
                inactive,
                app_ids,
                source: RegistrySource {
                    channel,
                    index,
                    trust_key,
                },
            },
        ),
        AppCatalogCommand::Upgrade {
            all,
            accept_install,
            app_ids,
            channel,
            index,
            trust_key,
            json: _,
        } => managed::upgrade(
            home,
            &AppUpgrade {
                all,
                accept_install,
                app_ids,
                source: RegistrySource {
                    channel,
                    index,
                    trust_key,
                },
            },
        ),
    }
}
