//! What `main` resolves before the node boots, and the tokio runtime it serves on.

/// Everything `main` resolves before the node boots and serves.
pub(crate) struct NodeRun {
    pub(crate) runtime: tokio::runtime::Runtime,
    pub(crate) home: std::path::PathBuf,
    pub(crate) data_dir: std::path::PathBuf,
    pub(crate) pid: u32,
    pub(crate) session_ledger: Option<lastdb_node::session_ledger::Ledger>,
    pub(crate) session_heartbeat: Option<lastdb_node::session_ledger::SessionHeartbeat>,
    pub(crate) socket_path: Option<std::path::PathBuf>,
    pub(crate) full_socket_path: Option<std::path::PathBuf>,
}

/// Multi-thread runtime: the accept loop itself runs on the main thread;
/// each connection's handler blocks on async core calls via this runtime.
/// Tokio workers collect on park. A UDS worker never parks on this
/// runtime; it collects at request end via `collect_finished_request_heap`.
pub(crate) fn build_runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_park(lastdb_node::allocator::collect_thread_heap_if_requested)
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))
}
