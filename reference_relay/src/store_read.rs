//! Log-entry reading and delivery cursor parsing for [`Store`].

use std::io::ErrorKind;

use serde_json::Value;
use tokio::fs;

use crate::error::{ApiError, ApiResult};
use crate::store::{is_hex64, require_hex64, EntryKey, Store, MAX_BATCH};

impl Store {
    /// Read a device chain from `from_seq`, returning opaque JSON values.
    pub(crate) async fn read_entries(
        &self,
        database_id: &str,
        node_key: &str,
        device_id: &str,
        from_seq: u64,
        limit: usize,
    ) -> ApiResult<(Vec<Value>, Option<String>)> {
        require_hex64(device_id, "entries.device_id")?;
        let limit = limit.clamp(1, MAX_BATCH);
        let mut entries = Vec::new();
        let mut seq = from_seq.max(1);
        if seq <= self.heads.floor(database_id, node_key, device_id) {
            return Err(ApiError::trimmed("from_seq"));
        }
        while entries.len() < limit {
            let meta = EntryKey {
                database_id: database_id.to_owned(),
                node_key: node_key.to_owned(),
                device_id: device_id.to_owned(),
                writer_seq: seq,
            };
            match fs::read(self.entry_path(&meta)).await {
                Ok(bytes) => {
                    let val =
                        serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("entries"))?;
                    entries.push(val);
                    seq += 1;
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    if seq <= self.heads.floor(database_id, node_key, device_id) {
                        return Err(ApiError::trimmed("from_seq"));
                    }
                    break;
                }
                Err(_) => return Err(ApiError::invalid("data_dir")),
            }
        }
        let cursor = if entries.len() == limit {
            Some(format!("d:{node_key}:{device_id}:{seq}"))
        } else {
            None
        };
        Ok((entries, cursor))
    }

    /// Read every device under `writer_key`, ordered by device id then seq.
    ///
    /// Trimmed streams start after their `floor` (`relay-protocol.md`: an all-devices read
    /// returns the entries that are stored).
    pub(crate) async fn read_writer_entries(
        &self,
        database_id: &str,
        writer_key: &str,
        from_device: Option<&str>,
        from_seq: u64,
        limit: usize,
    ) -> ApiResult<(Vec<Value>, Option<String>)> {
        require_hex64(writer_key, "writer_key")?;
        if let Some(device) = from_device {
            require_hex64(device, "cursor")?;
        }
        let limit = limit.clamp(1, MAX_BATCH);
        let devices = self.list_devices(database_id, writer_key).await?;
        let (start, mut resume_seq) = if let Some(device) = from_device {
            match devices.binary_search_by(|d| d.as_str().cmp(device)) {
                Ok(idx) => (idx, Some(from_seq.max(1))),
                Err(idx) => (idx, None),
            }
        } else {
            (0, Some(from_seq.max(1)))
        };
        let mut entries = Vec::new();
        for device_id in devices.iter().skip(start) {
            let floor = self.heads.floor(database_id, writer_key, device_id);
            let mut seq = resume_seq.take().unwrap_or(1).max(floor + 1).max(1);
            while entries.len() < limit {
                let meta = EntryKey {
                    database_id: database_id.to_owned(),
                    node_key: writer_key.to_owned(),
                    device_id: device_id.clone(),
                    writer_seq: seq,
                };
                match fs::read(self.entry_path(&meta)).await {
                    Ok(bytes) => {
                        let val = serde_json::from_slice(&bytes)
                            .map_err(|_| ApiError::invalid("entries"))?;
                        entries.push(val);
                        seq += 1;
                    }
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        let now_floor = self.heads.floor(database_id, writer_key, device_id);
                        if seq <= now_floor {
                            seq = now_floor + 1;
                            continue;
                        }
                        break;
                    }
                    Err(_) => return Err(ApiError::invalid("data_dir")),
                }
            }
            if entries.len() == limit {
                return Ok((entries, Some(format!("w:{writer_key}:{device_id}:{seq}"))));
            }
        }
        Ok((entries, None))
    }

    async fn list_devices(&self, database_id: &str, writer_key: &str) -> ApiResult<Vec<String>> {
        let dir = self.root.join(format!("logs/{database_id}/{writer_key}"));
        let mut rd = match fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => return Err(ApiError::invalid("writer_key")),
        };
        let mut devices = Vec::new();
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|_| ApiError::invalid("writer_key"))?
        {
            if !ent
                .file_type()
                .await
                .map_err(|_| ApiError::invalid("writer_key"))?
                .is_dir()
            {
                continue;
            }
            let name = ent.file_name().to_string_lossy().into_owned();
            if is_hex64(&name) {
                devices.push(name);
            }
        }
        devices.sort();
        Ok(devices)
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LogReadBody {
    pub(crate) database_id: String,
    pub(crate) writer_key: Option<String>,
    pub(crate) device_id: Option<String>,
    pub(crate) from_seq: Option<u64>,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<u64>,
}

pub(crate) fn read_limit(limit: Option<u64>) -> ApiResult<usize> {
    match limit {
        None => Ok(MAX_BATCH),
        Some(0) => Err(ApiError::invalid("limit")),
        Some(n) if n > MAX_BATCH as u64 => Err(ApiError::batch_too_large("limit")),
        Some(n) => Ok(n as usize),
    }
}

pub(crate) enum ReadPosition {
    /// One device chain.
    Device {
        writer_key: String,
        device_id: String,
        from_seq: u64,
    },
    /// Every device under `writer_key`; cursor carries the device.
    Writer {
        writer_key: String,
        from_device: Option<String>,
        from_seq: u64,
    },
}

pub(crate) fn read_position(req: &LogReadBody) -> ApiResult<ReadPosition> {
    if let Some(cursor) = req.cursor.as_deref() {
        if cursor.is_empty() || cursor.len() > 256 || req.from_seq.is_some() {
            return Err(ApiError::invalid("cursor"));
        }
        let parsed = parse_cursor(cursor)?;
        if parsed.single_device == Some(false) && req.device_id.is_some() {
            return Err(ApiError::invalid("cursor"));
        }
        if let Some(req_writer) = req.writer_key.as_deref() {
            require_hex64(req_writer, "writer_key")?;
            if req_writer != parsed.writer_key {
                return Err(ApiError::invalid("cursor"));
            }
        }
        if let Some(req_device) = req.device_id.as_deref() {
            require_hex64(req_device, "device_id")?;
            if req_device != parsed.device_id {
                return Err(ApiError::invalid("cursor"));
            }
        }
        let is_single = parsed.single_device.unwrap_or(req.device_id.is_some());
        return if is_single {
            Ok(ReadPosition::Device {
                writer_key: parsed.writer_key,
                device_id: parsed.device_id,
                from_seq: parsed.from_seq,
            })
        } else {
            Ok(ReadPosition::Writer {
                writer_key: parsed.writer_key,
                from_device: Some(parsed.device_id),
                from_seq: parsed.from_seq,
            })
        };
    }
    if req.from_seq == Some(0) {
        return Err(ApiError::invalid("from_seq"));
    }
    let writer_key = req
        .writer_key
        .clone()
        .ok_or_else(|| ApiError::invalid("writer_key"))?;
    require_hex64(&writer_key, "writer_key")?;
    let from_seq = req.from_seq.unwrap_or(1);
    if let Some(device_id) = req.device_id.as_deref() {
        require_hex64(device_id, "device_id")?;
        Ok(ReadPosition::Device {
            writer_key,
            device_id: device_id.to_owned(),
            from_seq,
        })
    } else {
        Ok(ReadPosition::Writer {
            writer_key,
            from_device: None,
            from_seq,
        })
    }
}

struct ParsedCursor {
    single_device: Option<bool>,
    writer_key: String,
    device_id: String,
    from_seq: u64,
}

/// Parse an opaque delivery cursor (`d:writer:device:seq`, `w:writer:device:seq`, or `writer:device:seq`).
fn parse_cursor(cursor: &str) -> ApiResult<ParsedCursor> {
    let (single_device, rest) = if let Some(rest) = cursor.strip_prefix("d:") {
        (Some(true), rest)
    } else if let Some(rest) = cursor.strip_prefix("w:") {
        (Some(false), rest)
    } else {
        (None, cursor)
    };
    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(ApiError::invalid("cursor"));
    }
    let from_seq: u64 = parts[2].parse().map_err(|_| ApiError::invalid("cursor"))?;
    if from_seq < 1 {
        return Err(ApiError::invalid("cursor"));
    }
    require_hex64(parts[0], "cursor")?;
    require_hex64(parts[1], "cursor")?;
    Ok(ParsedCursor {
        single_device,
        writer_key: parts[0].to_owned(),
        device_id: parts[1].to_owned(),
        from_seq,
    })
}
