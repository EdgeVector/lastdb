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

/// Tokio's default worker stack is 2 MiB. A debug build keeps the write
/// future on the stack, and the first mutation then aborts the worker
/// (`thread 'tokio-rt-worker' has overflowed its stack`). The install smoke
/// hit that abort on 2026-10-26 after `brain init` declared its schemas.
/// 16 MiB matches the offline recount runtime. The mapping is reserved and
/// committed page by page. `RUST_MIN_STACK` can only raise the floor.
const RUNTIME_STACK_BYTES: usize = 16 * 1024 * 1024;

fn runtime_stack_bytes() -> usize {
    std::env::var("RUST_MIN_STACK")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
        .max(RUNTIME_STACK_BYTES)
}

/// Multi-thread runtime: the accept loop itself runs on the main thread;
/// each connection's handler blocks on async core calls via this runtime.
/// Tokio workers collect on park. A UDS worker never parks on this
/// runtime; it collects at request end via `collect_finished_request_heap`.
pub(crate) fn build_runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(runtime_stack_bytes())
        .on_thread_park(lastdb_node::allocator::collect_thread_heap_if_requested)
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))
}
