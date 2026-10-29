use super::*;

/// Offline Search rebuild: page product records into apps/search/inbox.
pub(crate) fn search_rebuild_command(
    data_dir: Option<PathBuf>,
    page_size: usize,
    json_only: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    // Refuse if daemon holds the store (exclusive open would race).
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    if lastdb_node::health_alert::probe_health(&socket).is_ok() {
        return Err(format!(
            "lastdbd is running at {} — stop the daemon before offline search-rebuild (exclusive home open)",
            socket.display()
        ));
    }
    // A format v2 home derives its run home keys from the node key.
    let (e2e, _) = lastdb_node::offline_home::load_e2e_keys(&home)?;
    let node_key = e2e.encryption_key();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    let report = runtime
        .block_on(async {
            fold_db::db_operations::search_index::run_search_rebuild_for_home(
                &home, &node_key, page_size,
            )
            .await
        })
        .map_err(|e| format!("search rebuild: {e}"))?;
    if json_only {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "pages": report.pages,
                "batches": report.batches,
                "changes": report.changes,
                "inbox": report.inbox,
                "home": home,
            })
        );
    } else {
        println!(
            "search rebuild: pages={} batches={} changes={} inbox={}",
            report.pages,
            report.batches,
            report.changes,
            report.inbox.display()
        );
    }
    Ok(())
}
