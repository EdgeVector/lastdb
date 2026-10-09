//! Bounded best-effort progress delivery, shared by restore and startup.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

pub struct Events<T> {
    send: mpsc::SyncSender<Option<T>>,
    receive: mpsc::Receiver<Option<T>>,
}

#[derive(Clone)]
pub struct Notifier<T>(mpsc::SyncSender<Option<T>>);

pub struct Reporter<T> {
    send: mpsc::SyncSender<Option<T>>,
    stop: Arc<AtomicBool>,
    finished: mpsc::Receiver<()>,
}

pub fn channel<T>() -> (Notifier<T>, Events<T>) {
    let (send, receive) = mpsc::sync_channel(32);
    (Notifier(send.clone()), Events { send, receive })
}

impl<T> Notifier<T> {
    pub fn notify(&self, value: T) {
        let _ = self.0.try_send(Some(value));
    }
}

impl<T: Send + 'static> Events<T> {
    /// Snapshot callbacks release their state locks before the sink runs.
    /// Only phase events enter the queue; ordinary progress is sampled at 5s.
    pub fn start(
        self,
        name: &str,
        snapshot: impl Fn() -> Option<T> + Send + 'static,
        terminal: impl Fn(&T) -> bool + Send + 'static,
        mut sink: impl FnMut(&T) + Send + 'static,
    ) -> Reporter<T> {
        let Self { send, receive } = self;
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let (done, finished) = mpsc::channel();
        let _ = std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                loop {
                    let value = match receive.recv_timeout(Duration::from_secs(5)) {
                        Ok(value) if !worker_stop.load(Ordering::Acquire) => value,
                        _ => snapshot(),
                    };
                    if let Some(value) = value {
                        let is_terminal = terminal(&value);
                        sink(&value);
                        if is_terminal {
                            break;
                        }
                    }
                    if worker_stop.load(Ordering::Acquire) {
                        if let Some(value) = snapshot() {
                            sink(&value);
                        }
                        break;
                    }
                }
                let _ = done.send(());
            });
        Reporter {
            send,
            stop,
            finished,
        }
    }
}

impl<T> Reporter<T> {
    /// A stalled sink cannot delay the caller beyond this delivery budget.
    pub fn finish(self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.send.try_send(None);
        let _ = self.finished.recv_timeout(Duration::from_millis(100));
    }
}

impl<T> Drop for Reporter<T> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.send.try_send(None);
    }
}
