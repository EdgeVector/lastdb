//! Writer-scoped and whole-database views of the stream heads.
use super::{Devices, Heads};

impl Heads {
    /// Include streams whose entries are still beyond a gap in a deletion fence.
    pub(crate) fn stored_streams(&self, database_id: &str) -> Vec<(String, String)> {
        let streams = self.streams.lock().expect("heads lock");
        let mut found = Vec::new();
        for ((db, writer), devices) in streams.range((database_id.to_owned(), String::new())..) {
            if db != database_id {
                break;
            }
            found.extend(
                devices
                    .iter()
                    .filter(|(_, head)| head.seq > 0 || !head.pending.is_empty())
                    .map(|(device, _)| (writer.clone(), device.clone())),
            );
        }
        found
    }

    /// Contiguous streams in writer and device order. The writer is implicit for a scoped request.
    pub(crate) fn list(
        &self,
        database_id: &str,
        writer: Option<&str>,
    ) -> Vec<relay_protocol::StreamHead> {
        let streams = self.streams.lock().expect("heads lock");
        let mut found = Vec::new();
        let mut add = |key: &str, devices: &Devices| {
            found.extend(
                devices
                    .iter()
                    .filter(|(_, h)| h.seq > 0)
                    .map(|(device, h)| relay_protocol::StreamHead {
                        writer_key: writer.is_none().then(|| key.to_owned()),
                        device_id: device.clone(),
                        head_seq: h.seq,
                        head_hash: h.hash.clone(),
                    }),
            );
        };
        if let Some(writer) = writer {
            if let Some(devices) = streams.get(&(database_id.to_owned(), writer.to_owned())) {
                add(writer, devices);
            }
        } else {
            for ((db, key), devices) in streams.range((database_id.to_owned(), String::new())..) {
                if db != database_id {
                    break;
                }
                add(key, devices);
            }
        }
        found
    }
}
