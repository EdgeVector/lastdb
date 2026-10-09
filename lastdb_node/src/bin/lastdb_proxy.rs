//! LastDB Mini public-socket proxy for prepared worker cutovers.
//!
//! The proxy owns the stable public sockets. A `lastdbd` worker binds private
//! sockets with `lastdbd --socket-path ... --full-socket-path ...` and keeps
//! the one-process database ownership rule. The proxy sends each new request
//! to the current worker. A control socket changes the worker target after a
//! replacement worker passes its health checks.
//!
//! The proxy does not open a LastDB home. It cannot corrupt the store and it
//! cannot create a second database owner. During the short handoff gap it
//! returns HTTP 503 with a retry hint instead of exposing `ECONNREFUSED`.

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use lastdb_uds::uds::UdsSocket;
use serde::{Deserialize, Serialize};

const DEFAULT_QUEUE: usize = 256;
const DEFAULT_WORKERS: usize = 8;
const BUSY_RESPONSE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 51\r\nConnection: close\r\nRetry-After: 1\r\n\r\n{\"ok\":false,\"error\":\"proxy_busy\",\"retry_after_s\":1}";
const UNAVAILABLE_RESPONSE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 59\r\nConnection: close\r\nRetry-After: 1\r\n\r\n{\"ok\":false,\"error\":\"worker_unavailable\",\"retry_after_s\":1}";

#[derive(Parser, Debug)]
#[command(
    name = "lastdb-proxy",
    about = "Stable public sockets for LastDB worker cutovers"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the public proxy daemon.
    Serve(ServeArgs),
    /// Change the worker target through the proxy control socket.
    SetTarget(SetTargetArgs),
    /// Read the proxy target and active-connection count.
    Status(ControlArgs),
}

#[derive(Args, Clone, Debug)]
struct ServeArgs {
    /// Stable owner-socket path that the proxy owns.
    #[arg(long)]
    socket: PathBuf,

    /// Stable full-surface socket path. Omit when callers do not use it.
    #[arg(long)]
    full_socket: Option<PathBuf>,

    /// Private owner-socket path of the current worker.
    #[arg(long)]
    target: PathBuf,

    /// Private full-surface socket path of the current worker.
    #[arg(long)]
    target_full_socket: Option<PathBuf>,

    /// Proxy control socket path.
    #[arg(long)]
    control_socket: PathBuf,

    /// Number of forwarding workers.
    #[arg(long, default_value_t = DEFAULT_WORKERS)]
    workers: usize,

    /// Bounded queue size before the proxy returns 503.
    #[arg(long, default_value_t = DEFAULT_QUEUE)]
    queue: usize,
}

#[derive(Args, Clone, Debug)]
struct ControlArgs {
    /// Proxy control socket path.
    #[arg(long)]
    control_socket: PathBuf,
}

#[derive(Args, Clone, Debug)]
struct SetTargetArgs {
    #[command(flatten)]
    control: ControlArgs,

    /// Private owner-socket path of the ready worker.
    #[arg(long)]
    target: PathBuf,

    /// Private full-surface socket path of the ready worker.
    #[arg(long)]
    target_full_socket: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Target {
    socket: PathBuf,
    full_socket: Option<PathBuf>,
}

#[derive(Debug)]
struct State {
    target: RwLock<Target>,
    active: AtomicUsize,
}

#[derive(Debug)]
struct Connection {
    stream: UnixStream,
    full: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
enum ControlRequest {
    SetTarget {
        target: PathBuf,
        full_target: Option<PathBuf>,
    },
    Status,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    ok: bool,
    target: Target,
    active: usize,
}

fn main() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve(args),
        Command::SetTarget(args) => {
            let request = ControlRequest::SetTarget {
                target: args.target,
                full_target: args.target_full_socket,
            };
            send_control(&args.control.control_socket, &request)
        }
        Command::Status(args) => send_control(&args.control_socket, &ControlRequest::Status),
    }
}

fn serve(args: ServeArgs) -> Result<(), String> {
    if args.workers == 0 || args.queue == 0 {
        return Err("--workers and --queue must be greater than zero".to_string());
    }
    if args.full_socket.is_some() != args.target_full_socket.is_some() {
        return Err(
            "--full-socket and --target-full-socket must be set together, or both omitted"
                .to_string(),
        );
    }
    let public = bind_socket(&args.socket, "public")?;
    let public_full = args
        .full_socket
        .as_ref()
        .map(|path| bind_socket(path, "full public"))
        .transpose()?;
    let control = bind_socket(&args.control_socket, "control")?;
    let state = Arc::new(State {
        target: RwLock::new(Target {
            socket: args.target,
            full_socket: args.target_full_socket,
        }),
        active: AtomicUsize::new(0),
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    register_shutdown(&shutdown)?;

    let (sender, receiver) = mpsc::sync_channel::<Connection>(args.queue);
    let receiver = Arc::new(std::sync::Mutex::new(receiver));
    let mut threads = Vec::with_capacity(args.workers + 3);
    for _ in 0..args.workers {
        let receiver = Arc::clone(&receiver);
        let state = Arc::clone(&state);
        let shutdown = Arc::clone(&shutdown);
        threads.push(thread::spawn(move || loop {
            let connection = {
                let receiver = receiver.lock().expect("proxy receiver lock");
                receiver.recv_timeout(Duration::from_millis(100))
            };
            let Ok(connection) = connection else {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                continue;
            };
            forward(connection, &state);
        }));
    }

    threads.push(spawn_accept_loop(
        public,
        false,
        sender.clone(),
        Arc::clone(&shutdown),
    ));
    if let Some(public_full) = public_full {
        threads.push(spawn_accept_loop(
            public_full,
            true,
            sender.clone(),
            Arc::clone(&shutdown),
        ));
    }
    threads.push(spawn_control_loop(
        control,
        Arc::clone(&state),
        Arc::clone(&shutdown),
    ));

    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }
    drop(sender);
    for thread in threads {
        let _ = thread.join();
    }
    Ok(())
}

fn bind_socket(path: &Path, label: &str) -> Result<UdsSocket, String> {
    UdsSocket::bind_at(path.to_path_buf()).map_err(|error| {
        format!(
            "failed to bind {label} proxy socket {}: {error}",
            path.display()
        )
    })
}

fn spawn_accept_loop(
    socket: UdsSocket,
    full: bool,
    sender: mpsc::SyncSender<Connection>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = socket.listener().try_clone().expect("proxy listener clone");
        listener
            .set_nonblocking(true)
            .expect("proxy listener nonblocking");
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let connection = Connection { stream, full };
                    if let Err(error) = sender.try_send(connection) {
                        let mut stream = match error {
                            mpsc::TrySendError::Full(connection)
                            | mpsc::TrySendError::Disconnected(connection) => connection.stream,
                        };
                        let _ = stream.write_all(BUSY_RESPONSE);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => {
                    eprintln!("lastdb-proxy accept error: {error}");
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    })
}

fn spawn_control_loop(
    socket: UdsSocket,
    state: Arc<State>,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = socket
            .listener()
            .try_clone()
            .expect("proxy control listener clone");
        listener
            .set_nonblocking(true)
            .expect("proxy control listener nonblocking");
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => handle_control(&mut stream, &state),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => {
                    eprintln!("lastdb-proxy control accept error: {error}");
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    })
}

fn handle_control(stream: &mut UnixStream, state: &State) {
    let mut bytes = Vec::new();
    if stream.read_to_end(&mut bytes).is_err() {
        return;
    }
    let response = match serde_json::from_slice::<ControlRequest>(&bytes) {
        Ok(ControlRequest::Status) => serde_json::to_vec(&StatusResponse {
            ok: true,
            target: state.target.read().expect("proxy target lock").clone(),
            active: state.active.load(Ordering::Relaxed),
        }),
        Ok(ControlRequest::SetTarget {
            target,
            full_target,
        }) => {
            let probe = UnixStream::connect(&target);
            let full_probe = full_target.as_ref().map(UnixStream::connect).transpose();
            match (probe, full_probe) {
                (Ok(_), Ok(Some(_) | None)) => {
                    *state.target.write().expect("proxy target lock") = Target {
                        socket: target,
                        full_socket: full_target,
                    };
                    serde_json::to_vec(&serde_json::json!({ "ok": true }))
                }
                (Err(error), _) | (_, Err(error)) => serde_json::to_vec(&serde_json::json!({
                    "ok": false,
                    "error": "target_unavailable",
                    "detail": error.to_string(),
                })),
            }
        }
        Err(error) => serde_json::to_vec(&serde_json::json!({
            "ok": false,
            "error": "invalid_control_request",
            "detail": error.to_string(),
        })),
    };
    if let Ok(response) = response {
        let _ = stream.write_all(&response);
        let _ = stream.write_all(b"\n");
    }
}

fn forward(connection: Connection, state: &State) {
    state.active.fetch_add(1, Ordering::Relaxed);
    let result = forward_inner(connection, state);
    state.active.fetch_sub(1, Ordering::Relaxed);
    if let Err(error) = result {
        eprintln!("lastdb-proxy forwarding ended: {error}");
    }
}

fn forward_inner(connection: Connection, state: &State) -> io::Result<()> {
    let target = state.target.read().expect("proxy target lock").clone();
    let path = if connection.full {
        target.full_socket.as_ref().unwrap_or(&target.socket)
    } else {
        &target.socket
    };
    let mut worker = match UnixStream::connect(path) {
        Ok(worker) => worker,
        Err(error) => {
            let mut client = connection.stream;
            client.write_all(UNAVAILABLE_RESPONSE)?;
            return Err(error);
        }
    };
    let mut client = connection.stream;
    let mut worker_read = worker.try_clone()?;
    let mut client_read = client.try_clone()?;
    thread::scope(|scope| {
        let to_worker = scope.spawn(|| io::copy(&mut client_read, &mut worker));
        let to_client = scope.spawn(|| io::copy(&mut worker_read, &mut client));
        let _ = to_worker.join();
        let _ = to_client.join();
    });
    Ok(())
}

fn send_control(path: &Path, request: &ControlRequest) -> Result<(), String> {
    let mut stream = UnixStream::connect(path).map_err(|error| {
        format!(
            "cannot connect to proxy control socket {}: {error}",
            path.display()
        )
    })?;
    let body = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    stream.write_all(&body).map_err(|error| error.to_string())?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    println!("{}", response.trim());
    if response.contains("\"ok\":false") {
        return Err(response);
    }
    Ok(())
}

fn register_shutdown(shutdown: &Arc<AtomicBool>) -> Result<(), String> {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let flag = Arc::clone(shutdown);
        unsafe {
            signal_hook_registry::register(signal, move || {
                flag.store(true, Ordering::Relaxed);
            })
        }
        .map_err(|error| format!("failed to register signal handler: {error}"))?;
    }
    Ok(())
}
