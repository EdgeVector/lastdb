//! Pieces of the `lastdbd` binary, split out of `main.rs`.

pub(crate) mod serve;

pub(crate) mod cli {
    //! Command line of `lastdbd`.

    use std::io::Write;
    use std::path::PathBuf;

    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(
        name = "lastdbd",
        about = "LastDB Mini semantic daemon: core DB + app-identity + explicit byte backups over the owner Unix socket",
        // Stamped by build.rs (tag -> git describe -> manifest), matching the
        // release gate's --version-equals-tag assertion.
        version = env!("FOLDDB_BUILD_VERSION")
    )]
    pub(crate) struct Cli {
        /// Node home directory (default: LASTDB_HOME / service-home / ~/.lastdb).
        #[arg(long)]
        pub(crate) data_dir: Option<PathBuf>,

        /// Publish changes in this share home using the existing owner home.
        #[arg(long, requires = "data_dir")]
        pub(crate) share_owner_home: Option<PathBuf>,

        /// Worker socket path. The default is `<home>/data/folddb.sock`.
        #[arg(long)]
        pub(crate) socket_path: Option<PathBuf>,

        /// Enable bounded change delivery to LastSeek/Search. Off by default.
        /// A new or interrupted Search corpus needs a full rebuild from LastDB.
        #[arg(long)]
        pub(crate) enable_search: bool,
    }

    impl Cli {
        /// Preserve requested help/version output and argument-error exit status,
        /// without printing rejected paths or values into captured stderr.
        pub(crate) fn parse_private() -> Self {
            match Self::try_parse() {
                Ok(cli) => cli,
                Err(error) => {
                    if matches!(
                        error.kind(),
                        clap::error::ErrorKind::DisplayHelp
                            | clap::error::ErrorKind::DisplayVersion
                    ) {
                        error.exit();
                    }
                    let _ = writeln!(std::io::stderr().lock(), "lastdbd: invalid_arguments");
                    std::process::exit(2);
                }
            }
        }
    }
}

pub(crate) mod node_run {
    //! What `main` resolves before the node boots, and the tokio runtime it serves on.

    /// Everything `main` resolves before the node boots and serves.
    pub(crate) struct NodeRun {
        pub(crate) runtime: tokio::runtime::Runtime,
        pub(crate) home: std::path::PathBuf,
        pub(crate) data_dir: std::path::PathBuf,
        pub(crate) pid: u32,
        pub(crate) socket_path: Option<std::path::PathBuf>,
        pub(crate) share: Option<lastdb_node::share_publication::AutomaticPublication>,
    }

    /// Tokio's default worker stack is 2 MiB. A debug build keeps the write
    /// future on the stack and aborts the worker
    /// (`thread 'tokio-rt-worker' has overflowed its stack`). The install smoke
    /// hit that abort on 2026-10-26. 16 MiB was enough for schema init. Brain
    /// put then overflowed after a 3-mutation batch (run 40385637926). 64 MiB
    /// is the new floor. The mapping is reserved and committed page by page.
    /// `RUST_MIN_STACK` can only raise the floor.
    const RUNTIME_STACK_BYTES: usize = 64 * 1024 * 1024;

    fn runtime_stack_bytes() -> usize {
        std::env::var("RUST_MIN_STACK")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0)
            .max(RUNTIME_STACK_BYTES)
    }

    /// Multi-thread runtime: the accept loop itself runs on the main thread;
    /// each connection's handler blocks on async core calls via this runtime.
    pub(crate) fn build_runtime() -> Result<tokio::runtime::Runtime, String> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(runtime_stack_bytes())
            .build()
            .map_err(|e| format!("tokio runtime construction failed: {e}"))
    }
}

pub(crate) mod private_diagnostics {
    //! Fixed-field formatting for daemon stderr, separate from CLI product output.

    use std::fmt;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::fmt::format::Writer;
    use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
    use tracing_subscriber::registry::LookupSpan;

    pub(crate) struct PrivateFormat;

    impl<S, N> FormatEvent<S, N> for PrivateFormat
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        fn format_event(
            &self,
            _context: &FmtContext<'_, S, N>,
            mut writer: Writer<'_>,
            event: &Event<'_>,
        ) -> fmt::Result {
            let metadata = event.metadata();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_secs());
            write!(
                writer,
                "timestamp={timestamp} severity={} category={}",
                metadata.level(),
                category(metadata.target())
            )?;
            let mut metrics = Metrics::default();
            if metadata.target() == "lastdbd" {
                event.record(&mut metrics);
            }
            for (name, value) in metrics.values {
                write!(writer, " {name}={value}")?;
            }
            writeln!(writer)
        }
    }

    fn category(target: &str) -> &'static str {
        match target {
            "lastdbd::shutdown" => "shutdown",
            "lastdb_node::cloud" => "cloud",
            "observability::crash" => "panic_report",
            _ => "daemon",
        }
    }

    #[derive(Default)]
    struct Metrics {
        values: Vec<(&'static str, u64)>,
    }

    impl Visit for Metrics {
        fn record_u64(&mut self, field: &Field, value: u64) {
            if matches!(
                field.name(),
                "workers" | "queue_capacity" | "max_field_value_bytes" | "default" | "absolute_max"
            ) {
                self.values.push((field.name(), value));
            }
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            if let Ok(value) = u64::try_from(value) {
                self.record_u64(field, value);
            }
        }

        // Messages, errors, span context, and arbitrary field values never pass.
        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }
}

pub(crate) mod tracing_init {
    //! Tracing setup: fixed-field stderr and Sentry ERROR layer.

    pub(crate) fn init_tracing() -> Option<observability::layers::error::SentryGuard> {
        use observability::layers::error::{build_error_layer, OBS_SENTRY_RELEASE_ENV};
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        std::env::set_var(
            OBS_SENTRY_RELEASE_ENV,
            lastdb_node::version::build_version(),
        );

        let initial_directive = std::env::var("RUST_LOG")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .filter(|v| tracing_subscriber::EnvFilter::try_new(v).is_ok())
            .unwrap_or_else(|| "info".to_string());
        let env_filter = tracing_subscriber::EnvFilter::try_new(&initial_directive)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .event_format(crate::lastdbd::private_diagnostics::PrivateFormat);
        let (error_layer, sentry_guard) = match build_error_layer() {
            Some((layer, guard)) => (Some(layer), Some(guard)),
            None => (None, None),
        };

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(error_layer)
            .init();

        sentry_guard
    }
}

pub(crate) mod background {
    //! Background ownership starts only after the daemon can serve both sockets.

    use lastdb_node::host::Host;
    use std::sync::{atomic::AtomicBool, Arc};
    use tokio::task::JoinHandle;

    type BackgroundTasks = (
        JoinHandle<()>,
        JoinHandle<()>,
        JoinHandle<()>,
        JoinHandle<()>,
    );

    pub(crate) fn spawn(
        runtime: &tokio::runtime::Runtime,
        host: &Arc<Host>,
        shutdown: &Arc<AtomicBool>,
        share: Option<lastdb_node::share_publication::AutomaticPublication>,
    ) -> Result<BackgroundTasks, String> {
        let _runtime_guard = runtime.enter();
        // fetch-blob downloads a record file that is not local (after a restore)
        // when this home has saved cloud credentials.
        lastdb_node::byte_cloud_backup::register_blob_source(host);
        let backup = match share {
            Some(share) => share.spawn(Arc::clone(host), Arc::clone(shutdown))?,
            None => lastdb_node::byte_cloud_backup::spawn_automatic(
                Arc::clone(host),
                Arc::clone(shutdown),
            ),
        };
        Ok((
            lastdb_node::jobs::ttl_sweep::spawn_periodic(Arc::clone(host), Arc::clone(shutdown)),
            backup,
            lastdb_node::shared_sync::spawn_automatic(Arc::clone(host), Arc::clone(shutdown)),
            lastdb_node::registered_share::spawn(host.home.clone(), Arc::clone(shutdown)),
        ))
    }
}

pub(crate) mod shutdown {
    //! Graceful shutdown: drain budgets, background drain, and the flush proof.

    use std::sync::Arc;

    use lastdb_node::host::Host;

    pub(crate) fn shutdown_handler_drain_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(180)
    }

    pub(crate) fn complete_shutdown_proof(
        home: &std::path::Path,
        pid: u32,
        start_ts: u64,
        errors: &[String],
    ) -> Result<(), String> {
        if !errors.is_empty() {
            return Err(format!(
                "shutdown did not prove a complete flush: {}",
                errors.join("; ")
            ));
        }
        lastdb_node::shutdown_receipt::write_shutdown_flush_receipt(home, pid, start_ts)
            .map_err(|error| format!("could not write shutdown flush proof: {error}"))?;
        Ok(())
    }

    /// Settle every owned task, including after a sibling failure, before the flush.
    pub(crate) fn drain_background(
        runtime: &tokio::runtime::Runtime,
        host: &Arc<Host>,
        ttl_sweep_task: tokio::task::JoinHandle<()>,
        automatic_backup_task: tokio::task::JoinHandle<()>,
        shared_sync_task: tokio::task::JoinHandle<()>,
        received_shares_task: tokio::task::JoinHandle<()>,
    ) -> Result<(), String> {
        runtime.block_on(async {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
            let drain = async {
                let (ttl_sweep, automatic, shared_sync, received_shares, byte_backup) = tokio::join!(
                    ttl_sweep_task,
                    automatic_backup_task,
                    shared_sync_task,
                    received_shares_task,
                    host.byte_backup.drain(deadline),
                );
                let mut errors = Vec::new();
                for (name, result) in [
                    ("ttl sweep", ttl_sweep),
                    ("automatic backup", automatic),
                    ("shared sync", shared_sync),
                    ("received shares", received_shares),
                ] {
                    if let Err(error) = result {
                        errors.push(format!("{name} task failed: {error}"));
                    }
                }
                if let Err(error) = byte_backup {
                    errors.push(format!("byte backup did not drain: {error}"));
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.join("; "))
                }
            };
            tokio::time::timeout_at(deadline, drain)
                .await
                .map_err(|_| "background writers did not drain within 180 seconds".to_string())?
        })
    }

    /// Complete every local flush barrier after the writers drain.
    pub(crate) fn flush_database(
        runtime: &tokio::runtime::Runtime,
        host: &Host,
    ) -> Result<(), String> {
        runtime
            .block_on(host.db.shutdown())
            .map_err(|error| format!("FoldDB shutdown flush failed: {error}"))
    }
}
