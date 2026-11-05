//! Anonymous S3 catalog reads for the running node's exact interface.

use super::*;
use lastdb_node::app_registry_index as registry;

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
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
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

pub(crate) fn print_public_list(list: &registry::InterfaceList, json: bool) {
    if json {
        print_pretty_json(list);
        return;
    }
    println!(
        "# {} API {} · sequence {} · generated {}",
        list.channel, list.api_version, list.seq, list.generated_at
    );
    for entry in &list.apps {
        println!("{}\t{}\t{}", entry.app_id, entry.app_version, entry.sha);
    }
}
