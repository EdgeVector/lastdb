//! A bounded TCP relay. Requests and responses reach the real S3-compatible
//! server unless a case explicitly injects a 412 or loses a committed reply.

use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Default)]
pub struct Faults {
    pub puts: AtomicUsize,
    pub actual_conflicts: AtomicUsize,
    pub injected_conflicts: AtomicUsize,
    pub injected_request_conflicts: AtomicUsize,
    pub injected_failures: AtomicUsize,
    pub committed_lost_replies: AtomicUsize,
    reject_puts: AtomicUsize,
    conflict_puts: AtomicUsize,
    lose_reply_at: AtomicUsize,
    fail_put_at: AtomicUsize,
    gate: Mutex<Option<Arc<PairGate>>>,
    errors: Mutex<Vec<String>>,
}

struct PairGate {
    count: Mutex<usize>,
    ready: Condvar,
}

impl PairGate {
    fn arrive(&self) -> io::Result<()> {
        let mut count = self.count.lock().expect("gate count");
        *count += 1;
        self.ready.notify_all();
        let (count, timeout) = self
            .ready
            .wait_timeout_while(count, Duration::from_secs(15), |count| *count < 2)
            .expect("gate wait");
        if timeout.timed_out() && *count < 2 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "second conditional PUT never arrived",
            ));
        }
        Ok(())
    }
}

impl Faults {
    pub fn pair_next_puts(&self) {
        *self.gate.lock().expect("gate") = Some(Arc::new(PairGate {
            count: Mutex::new(0),
            ready: Condvar::new(),
        }));
    }

    pub fn reject_next_puts(&self, count: usize) {
        self.reject_puts.store(count, Ordering::SeqCst);
    }

    pub fn conflict_next_put(&self) {
        self.conflict_puts.store(1, Ordering::SeqCst);
    }

    pub fn lose_next_committed_reply(&self) {
        self.lose_reply_at
            .store(self.puts.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    }

    pub fn fail_nth_next_put(&self, number: usize) {
        self.fail_put_at
            .store(self.puts.load(Ordering::SeqCst) + number, Ordering::SeqCst);
    }

    pub fn assert_clean(&self) {
        let errors = self.errors.lock().expect("errors");
        assert!(errors.is_empty(), "proxy I/O failure: {errors:?}");
    }

    fn before_put(&self) -> io::Result<bool> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().expect("gate").clone();
        if let Some(gate) = gate {
            gate.arrive()?;
        }
        Ok(self
            .reject_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok())
    }
}

pub struct Proxy {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    pub faults: Arc<Faults>,
}

impl Proxy {
    pub fn new(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let stop = Arc::new(AtomicBool::new(false));
        let faults = Arc::new(Faults::default());
        let stopped = stop.clone();
        let observed = faults.clone();
        let worker = thread::spawn(move || {
            let mut sessions = Vec::new();
            while !stopped.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let observed = observed.clone();
                        sessions.push(thread::spawn(move || {
                            if let Err(error) = serve(client, upstream, &observed) {
                                observed
                                    .errors
                                    .lock()
                                    .expect("errors")
                                    .push(error.to_string());
                            }
                        }));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => {
                        observed
                            .errors
                            .lock()
                            .expect("errors")
                            .push(error.to_string());
                        break;
                    }
                }
            }
            for session in sessions {
                session.join().expect("proxy session");
            }
        });
        Self {
            address,
            stop,
            worker: Some(worker),
            faults,
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("proxy worker");
        }
    }
}

fn read_headers(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut prefix = Vec::new();
    let mut buffer = [0; 4096];
    while !prefix.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
        let count = stream.read(&mut buffer)?;
        if count == 0 || prefix.len() + count > 65536 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing or oversized HTTP headers",
            ));
        }
        prefix.extend_from_slice(&buffer[..count]);
    }
    Ok(prefix)
}

fn close_connection_header(prefix: &[u8]) -> io::Result<Vec<u8>> {
    let end = prefix
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("header end");
    let headers = std::str::from_utf8(&prefix[..end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 headers"))?;
    let mut output = Vec::new();
    for line in headers.split("\r\n") {
        if !line.to_ascii_lowercase().starts_with("connection:") {
            output.extend_from_slice(line.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
    }
    output.extend_from_slice(b"Connection: close\r\n\r\n");
    output.extend_from_slice(&prefix[end + 4..]);
    Ok(output)
}

fn serve(mut client: TcpStream, upstream: SocketAddr, faults: &Faults) -> io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(20)))?;
    client.set_write_timeout(Some(Duration::from_secs(20)))?;
    let request = read_headers(&mut client)?;
    let is_put = request.starts_with(b"PUT ");
    if is_put && faults.before_put()? {
        faults.injected_conflicts.fetch_add(1, Ordering::SeqCst);
        reject(
            &mut client,
            &request,
            "412 Precondition Failed",
            "PreconditionFailed",
        )?;
        return Ok(());
    }
    if is_put
        && faults
            .conflict_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok()
    {
        faults
            .injected_request_conflicts
            .fetch_add(1, Ordering::SeqCst);
        reject(
            &mut client,
            &request,
            "409 Conflict",
            "ConditionalRequestConflict",
        )?;
        return Ok(());
    }
    let number = faults.puts.load(Ordering::SeqCst);
    if is_put && number == faults.fail_put_at.load(Ordering::SeqCst) {
        faults.injected_failures.fetch_add(1, Ordering::SeqCst);
        reject(
            &mut client,
            &request,
            "500 Internal Server Error",
            "InternalError",
        )?;
        return Ok(());
    }
    let lose_reply = is_put && number == faults.lose_reply_at.load(Ordering::SeqCst);
    relay(client, request, upstream, faults, lose_reply)
}

/// The current smithy HTTP client resets its socket after it has read a
/// `Connection: close` response. The relay may still be copying the tail.
fn ignore_client_close(error: io::Error) -> io::Result<()> {
    match error.kind() {
        io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => Ok(()),
        _ => Err(error),
    }
}

fn write_response(
    client: &mut TcpStream,
    upstream: &mut TcpStream,
    response: &[u8],
) -> io::Result<()> {
    if let Err(error) = client.write_all(response) {
        return ignore_client_close(error);
    }
    match io::copy(upstream, client) {
        Ok(_) => Ok(()),
        Err(error) => ignore_client_close(error),
    }
}

fn reject(client: &mut TcpStream, request: &[u8], status: &str, code: &str) -> io::Result<()> {
    let end = request
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("headers");
    let headers = std::str::from_utf8(&request[..end]).expect("ASCII request");
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<u64>().expect("length"))
        })
        .expect("known ByteStream length");
    let remaining = length.saturating_sub((request.len() - end - 4) as u64);
    io::copy(&mut (&mut *client).take(remaining), &mut io::sink())?;
    let body =
        format!("<Error><Code>{code}</Code><Message>injected write failure</Message></Error>");
    write!(client, "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
}

fn relay(
    mut client: TcpStream,
    request: Vec<u8>,
    address: SocketAddr,
    faults: &Faults,
    lose_reply: bool,
) -> io::Result<()> {
    let mut upstream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    upstream.set_read_timeout(Some(Duration::from_secs(20)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(20)))?;
    upstream.write_all(&close_connection_header(&request)?)?;
    let mut source = client.try_clone()?;
    let mut destination = upstream.try_clone()?;
    let send_body = thread::spawn(move || io::copy(&mut source, &mut destination));
    let result = (|| {
        let response = read_headers(&mut upstream)?;
        let status = std::str::from_utf8(&response)
            .unwrap_or_default()
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default();
        if status == "412" {
            faults.actual_conflicts.fetch_add(1, Ordering::SeqCst);
        }
        if lose_reply {
            assert_eq!(
                status, "200",
                "lost reply must follow a real successful PUT"
            );
            io::copy(&mut upstream, &mut io::sink())?;
            faults.committed_lost_replies.fetch_add(1, Ordering::SeqCst);
        } else {
            write_response(&mut client, &mut upstream, &response)?;
        }
        Ok(())
    })();
    let _ = client.shutdown(Shutdown::Both);
    let _ = upstream.shutdown(Shutdown::Both);
    let _ = send_body.join().expect("request body relay");
    result
}
