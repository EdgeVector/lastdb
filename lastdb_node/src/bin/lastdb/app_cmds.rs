use super::*;
use lastdb_node::{app_publish, app_registry_index as registry};

#[path = "app_cmds/interface_publish.rs"]
mod interface_publish;
#[path = "app_cmds/managed.rs"]
mod managed;

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

fn warn_trust_override(source: &registry::TrustSource) {
    match source {
        registry::TrustSource::Pinned => {}
        registry::TrustSource::Flag => eprintln!("note: index trust key overridden by --trust-key"),
        registry::TrustSource::Env => {
            eprintln!(
                "note: index trust key overridden by ${}",
                registry::TRUST_KEY_ENV
            );
        }
    }
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

pub(super) fn app_command(data_dir: Option<PathBuf>, action: AppCommand) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        AppCommand::Managed(command) => managed::command(&home, command),
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
    let list = public_list(home, ch, idx, tk)?;
    let Some(app_id) = app_id else {
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
        return Ok(());
    };
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
    if catalog_entry || json {
        print_pretty_json(entry);
        return Ok(());
    }
    let channel = ch.unwrap_or(registry::DEFAULT_CHANNEL);
    let location = registry::IndexLocation::resolve(idx)?;
    let (trust, trust_source) = registry::resolve_list_trust_key(tk)?;
    let socket = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    let (node, node_source, _) = registry::running_node(&socket);
    let trust_label = match trust_source {
        registry::TrustSource::Pinned => "pinned".to_string(),
        registry::TrustSource::Flag => "flag".to_string(),
        registry::TrustSource::Env => format!("env:{}", registry::TRUST_KEY_ENV),
    };
    println!(
        "{} {} @ {} proved with lastdb {node} ({node_source}) at {}",
        entry.app_id, entry.app_version, entry.sha, entry.published_at
    );
    println!("  source: {}", entry.source);
    println!(
        "  index: {} (trust: {}:{})",
        location.describe_interface(channel, list.api_version),
        trust_label,
        app_identity_crypto::key_id(&trust)
    );
    Ok(())
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
