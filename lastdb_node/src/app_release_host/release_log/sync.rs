use super::*;

// ─── Sync ─────────────────────────────────────────────────────────────────

/// What one sync did.
#[derive(Debug, Clone, Serialize)]
pub struct SyncOutcome {
    /// The seq this node kept before the sync.
    pub from_seq: u64,
    /// The seq this node keeps now.
    pub to_seq: u64,
    /// The time of the head this node keeps now.
    pub head_time: String,
}

/// One new entry: its tile line, the parsed entry, and its hash.
type NewEntry = (String, LogEntry, String);

impl ReleaseLog {
    /// Read the head and the new entries, check them, and keep them.
    ///
    /// The checks are the node check of the format, steps 1 to 5: a trusted
    /// key and a good signature on the head, no step back from the kept
    /// seq, each new entry in sequence and chained to the one before, and
    /// the head hash on entry `head.seq`. Nothing is kept unless all pass,
    /// so a failed sync leaves the kept log as it was.
    ///
    /// # Errors
    /// No trusted key for the environment, a transport failure, or the
    /// first check that fails.
    pub async fn sync(&mut self) -> Result<SyncOutcome, String> {
        let keys = self.config.verifying_keys()?;
        let client = log_client()?;
        let head_url = format!("{}/head.json", self.config.base_url);
        let head: SignedHead = serde_json::from_slice(&fetch(&client, &head_url).await?)
            .map_err(|e| format!("{head_url} does not parse: {e}"))?;
        head.verify(&keys)?;
        let from_seq = self.seq();
        if head.seq < from_seq {
            return Err(format!(
                "the release log went back: its head covers {} entries, this node keeps \
                 {from_seq}",
                head.seq
            ));
        }
        let fresh = self.read_new_entries(&client, head.seq).await?;
        let last_hash = fresh
            .last()
            .map_or_else(|| self.last_hash(), |(_, _, hash)| hash.as_str());
        if last_hash != head.hash {
            return Err(format!(
                "the release log head hash does not match entry {}",
                head.seq
            ));
        }
        let entries_changed = !fresh.is_empty();
        for (line, entry, hash) in fresh {
            self.push(line, entry, hash);
        }
        // Two heads for the same seq are both true; the later one proves the
        // log was current later. Keep it, so a replayed old head never makes
        // the kept head older.
        let newer = match &self.head {
            Some(kept) if kept.seq == head.seq => kept.signed_at()? < head.signed_at()?,
            _ => true,
        };
        if newer {
            self.head = Some(head);
        }
        self.persist(entries_changed)?;
        Ok(SyncOutcome {
            from_seq,
            to_seq: self.seq(),
            head_time: self
                .head
                .as_ref()
                .map_or_else(String::new, |h| h.time.clone()),
        })
    }

    /// Entries `seq() + 1 ..= head_seq`, read from their tiles and checked.
    /// Tile lines past `head_seq` are ignored until a head covers them.
    async fn read_new_entries(
        &self,
        client: &reqwest::Client,
        head_seq: u64,
    ) -> Result<Vec<NewEntry>, String> {
        let mut fresh: Vec<NewEntry> = Vec::new();
        let mut next = self.seq() + 1;
        if head_seq < next {
            return Ok(fresh);
        }
        let mut prev = self.last_hash().to_string();
        let first_tile = (next - 1) / RELEASE_LOG_TILE_SIZE;
        let last_tile = (head_seq - 1) / RELEASE_LOG_TILE_SIZE;
        for tile in first_tile..=last_tile {
            let url = format!("{}/tiles/{tile}.jsonl", self.config.base_url);
            let text = String::from_utf8(fetch(client, &url).await?)
                .map_err(|e| format!("{url} is not UTF-8: {e}"))?;
            let mut seq = tile * RELEASE_LOG_TILE_SIZE;
            for line in text.split('\n').filter(|line| !line.is_empty()) {
                seq += 1;
                if seq < next {
                    continue;
                }
                if seq > head_seq {
                    break;
                }
                let (entry, hash) = parse_tile_line(line, seq, &prev)?;
                prev.clone_from(&hash);
                fresh.push((line.to_string(), entry, hash));
                next += 1;
            }
            let tile_end = ((tile + 1) * RELEASE_LOG_TILE_SIZE).min(head_seq);
            if next <= tile_end {
                return Err(format!(
                    "release log tile {tile} ends before entry {next}, which the head covers"
                ));
            }
        }
        Ok(fresh)
    }
}

fn log_client() -> Result<reqwest::Client, String> {
    // trace-egress: propagate (public release log reads: the signed head and
    // its tiles, from the verifier's own log host)
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))
}

/// GET one log object. A `file://` URL reads from disk, so a log can be
/// served from a directory.
async fn fetch(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    if let Some(path) = url.strip_prefix("file://") {
        return std::fs::read(path).map_err(|e| format!("failed to read {path}: {e}"));
    }
    // trace-egress: propagate (public release log read)
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    let status = response.status().as_u16();
    if status != 200 {
        return Err(format!("GET {url} returned {status}"));
    }
    response
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|e| format!("GET {url}: body read failed: {e}"))
}
