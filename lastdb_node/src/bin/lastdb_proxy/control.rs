use super::*;

pub(super) fn spawn_control_loop(
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

pub(super) fn handle_control(stream: &mut UnixStream, state: &State) {
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

pub(super) fn forward(connection: Connection, state: &State) {
    state.active.fetch_add(1, Ordering::Relaxed);
    let result = forward_inner(connection, state);
    state.active.fetch_sub(1, Ordering::Relaxed);
    if let Err(error) = result {
        eprintln!("lastdb-proxy forwarding ended: {error}");
    }
}

pub(super) fn forward_inner(connection: Connection, state: &State) -> io::Result<()> {
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

pub(super) fn send_control(path: &Path, request: &ControlRequest) -> Result<(), String> {
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

pub(super) fn register_shutdown(shutdown: &Arc<AtomicBool>) -> Result<(), String> {
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
