use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogKind {
    NativeMutationLog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationLogRoot {
    pub version: u16,
    pub kind: LogKind,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub recovery: RecoveryDescriptor,
    pub base: AcceptedBackup,
    pub previous: AcceptedBackup,
    pub anchor: LogAnchor,
    pub from: LogPosition,
    pub through: LogPosition,
    pub entries: Vec<LogEntry>,
    pub native_file_count: u64,
    pub native_bytes: u64,
    pub pages: Vec<ClientPageReference>,
}

impl MutationLogRoot {
    pub fn validate_for(&self, begin: &Begin) -> Result<()> {
        if let Some(dependencies) = &begin.recovery_dependencies {
            let mut expected = vec![self.base.head()];
            if self.previous != self.base {
                expected.push(self.previous.head());
            }
            if *dependencies != expected {
                return Err(LogCodecError("mutation log declared recovery dependencies"));
            }
        }
        context(
            self.version,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        self.base
            .validate()
            .map_err(|_| LogCodecError("mutation log base selection"))?;
        self.previous
            .validate()
            .map_err(|_| LogCodecError("mutation log previous selection"))?;
        self.recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(|_| LogCodecError("mutation log recovery identity"))?;
        if self.base.job().stream != begin.job.stream
            || self.previous.job().stream != begin.job.stream
            || begin.predecessor.as_ref() != Some(&self.previous.head())
            || self.base.ordinal.get() > self.previous.ordinal.get()
            || self.entries.is_empty()
            || self.entries.len() > MAX_BATCH_ENTRIES
            || self.from.epoch != self.anchor.epoch
            || self.through.epoch != self.anchor.epoch
            || self.pages.is_empty()
            || self.pages.len() > MAX_CLIENT_PAGES
        {
            return Err(LogCodecError(
                "mutation log root selection or resource bound",
            ));
        }
        let anchor = self
            .anchor
            .position()
            .map_err(|_| LogCodecError("mutation log anchor"))?;
        if (self.from.sequence == 0 || self.previous == self.base) && self.from != anchor {
            return Err(LogCodecError("mutation log snapshot boundary"));
        }
        let mut position = self.from.clone();
        let mut files = 0u64;
        let mut bytes = 0u64;
        for entry in &self.entries {
            position = entry
                .position_after(&position)
                .map_err(|_| LogCodecError("mutation log sequence or hash"))?;
            files = files
                .checked_add(entry.parts.len() as u64)
                .ok_or(LogCodecError("mutation log file count"))?;
            bytes = bytes
                .checked_add(
                    entry
                        .bytes()
                        .map_err(|_| LogCodecError("mutation log entry size"))?,
                )
                .ok_or(LogCodecError("mutation log byte count"))?;
        }
        if position != self.through || files != self.native_file_count || bytes != self.native_bytes
        {
            return Err(LogCodecError("mutation log complete range totals"));
        }
        for (index, page) in self.pages.iter().enumerate() {
            if page.page_index as usize != index
                || page.bytes.get() <= 29
                || page.bytes.get() > CODEC_ENVELOPE_MAX_BYTES
            {
                return Err(LogCodecError("mutation log page identity or length"));
            }
        }
        encoded_bound(self)
    }

    pub fn validate_page(&self, page: &MutationLogPage) -> Result<()> {
        if page.version != self.version
            || page.begin_sha256 != self.begin_sha256
            || page.descriptor_sha256 != self.descriptor_sha256
            || self.pages.get(page.page_index as usize).is_none()
        {
            return Err(LogCodecError("mutation log page context"));
        }
        for file in &page.files {
            self.validate_file(file)?;
        }
        Ok(())
    }

    pub fn validate_file(&self, file: &ClientNativeFile) -> Result<Destination> {
        file.validate()
            .map_err(|_| LogCodecError("mutation log exact file extents"))?;
        for entry in &self.entries {
            for part in &entry.parts {
                if file.path.as_str()
                    == log_path(&self.anchor.epoch, entry.sequence, part.role, &part.run_id)
                {
                    if file.native_bytes.get() != part.bytes {
                        return Err(LogCodecError("mutation log original file length"));
                    }
                    return Ok(if part.role.is_cas() {
                        Destination::B2
                    } else {
                        Destination::R2
                    });
                }
            }
        }
        Err(LogCodecError("mutation log unexpected native path"))
    }

    pub fn validate_previous(&self, previous: &Self) -> Result<()> {
        if self.base != previous.base
            || self.anchor != previous.anchor
            || self.from != previous.through
        {
            return Err(LogCodecError("mutation log predecessor prefix differs"));
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        let value: Self = decode(bytes, key)?;
        value.validate_for(begin)?;
        Ok(value)
    }
}
