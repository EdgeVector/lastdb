//! Bounded handoff to the existing blocking native importer.
use super::{RestoreError, Result};
use std::io::{self, Cursor, Read};
use tokio::sync::mpsc;

pub(super) type Sender = mpsc::Sender<Vec<u8>>;

pub(super) struct Input {
    receiver: mpsc::Receiver<Vec<u8>>,
    current: Cursor<Vec<u8>>,
}

pub(super) fn channel() -> (Sender, Input) {
    let (sender, receiver) = mpsc::channel(2);
    (
        sender,
        Input {
            receiver,
            current: Cursor::new(Vec::new()),
        },
    )
}

pub(super) async fn send(sender: &Sender, bytes: Vec<u8>) -> Result<()> {
    sender
        .send(bytes)
        .await
        .map_err(|_| RestoreError::Invalid("native record importer stopped"))
}

impl Read for Input {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let count = self.current.read(output)?;
            if count != 0 {
                return Ok(count);
            }
            match self.receiver.blocking_recv() {
                Some(bytes) => self.current = Cursor::new(bytes),
                None => return Ok(0),
            }
        }
    }
}
