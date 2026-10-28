//! Isolated checkpoint frames use their admitted small-block decoder window.
use super::*;
use crate::disk_run::footer::{Footer, Top, FOOTER_BYTES};
use crate::disk_run::{MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER};
use crate::read_receipt_io::{self as measured, ReadClass};
use std::path::Path;
use uuid::Uuid;

/// One active decoder: two GCM buffers, decoder workspace, window and output.
pub(super) const FRAME_WORKSPACE: u64 = 8 * 1024 * 1024;

pub(super) fn cap(stage: &'static str) -> Error {
    Error::Config(format!("checkpoint audit cap: {stage}"))
}

pub(super) fn admitted<T>(result: Result<T>, stage: &'static str) -> Result<T> {
    result.map_err(|error| match error {
        Error::ReadBudgetExceeded => cap(stage),
        error => error,
    })
}

pub(super) fn open_reader(path: &Path, key: &[u8; 32], allowance: u64) -> Result<RunReader> {
    let named = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(RUN_SUFFIX))
        .and_then(|name| Uuid::parse_str(name).ok());
    let named = named.ok_or_else(|| corrupt("diagnostic run name"))?;
    let file = measured::open(path)?;
    let bytes = file.metadata()?.len();
    if bytes < FOOTER_BYTES as u64 {
        return Err(corrupt("diagnostic run footer"));
    }
    let _class = ReadClass::Footer.enter();
    let mut raw = [0; FOOTER_BYTES];
    measured::read_exact_at(&file, &mut raw, bytes - FOOTER_BYTES as u64)?;
    let footer = Footer::decode(&raw)?;
    if footer.run_id != named {
        return Err(corrupt("diagnostic run identity"));
    }
    let end = footer
        .top_offset
        .checked_add(u64::from(footer.top_len))
        .and_then(|end| end.checked_add(FOOTER_BYTES as u64));
    if end != Some(bytes) {
        return Err(corrupt("diagnostic top extent"));
    }
    if u64::from(footer.max_payload) > allowance / 8 {
        return Err(cap("run_payload_allowance"));
    }
    let mut reader = RunReader {
        reserved_memory: 0,
        file,
        data_key: *key,
        frame_key: None,
        footer,
        top: Top {
            records: 0,
            data_blocks: 0,
            max_payload: 0,
            indexes: Vec::new(),
            last_key: Vec::new(),
        },
    };
    // The top is written after max_payload is finalized; give it its own cap.
    let maximum = usize::try_from((allowance / 32).min(MAX_TOP_BYTES as u64))
        .map_err(|_| cap("top_payload_allowance"))?;
    let payload = reader.diagnostic_frame(
        footer.top_offset,
        footer.top_len,
        TOP_COUNTER,
        maximum,
        "top_frame",
    )?;
    reader.top = admitted(Top::decode_bounded(&payload, allowance / 8), "top_metadata")?;
    if !reader.top.agrees_with(&footer) {
        return Err(corrupt("diagnostic top summary"));
    }
    Ok(reader)
}

impl RunReader {
    pub(in crate::disk_run::reader::stream) fn diagnostic_frame(
        &self,
        offset: u64,
        bytes: u32,
        counter: u64,
        maximum: usize,
        stage: &'static str,
    ) -> Result<Vec<u8>> {
        if maximum == 0
            || bytes as usize > maximum.saturating_add(8 + crate::frame::min_encoded_len())
        {
            return Err(cap(stage));
        }
        let window = maximum
            .max(1024)
            .checked_next_power_of_two()
            .ok_or_else(|| cap("decoder_window"))? as u64;
        let buffers = 4 * lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        if window
            .saturating_add(buffers)
            .saturating_add(maximum as u64)
            > FRAME_WORKSPACE
        {
            return Err(cap("decoder_workspace"));
        }
        let mut input = admitted(
            VerifiedFrameReader::open_with_window(
                &self.file,
                offset,
                u64::from(bytes),
                &self.data_key,
                maximum,
            ),
            stage,
        )?;
        if input.header().chunk_uuid != self.footer.run_id || input.header().counter != counter {
            return Err(corrupt("diagnostic frame identity"));
        }
        if input.payload_len() > maximum as u64 {
            return Err(cap(stage));
        }
        if input.memory_bytes().saturating_add(maximum as u64) > FRAME_WORKSPACE {
            return Err(cap("decoder_workspace"));
        }
        let count = usize::try_from(input.payload_len()).map_err(|_| cap(stage))?;
        let mut output = Vec::new();
        output.try_reserve_exact(count).map_err(|_| cap(stage))?;
        if output.capacity() > maximum {
            return Err(cap(stage));
        }
        output.resize(count, 0);
        input.read_exact(&mut output)?;
        let mut extra = [0; 1];
        if input.read(&mut extra)? != 0 {
            return Err(corrupt("diagnostic frame length"));
        }
        Ok(output)
    }

    pub(in crate::disk_run::reader::stream) fn diagnostic_index(
        &self,
        number: usize,
        allowance: u64,
    ) -> Result<super::super::IndexBlock> {
        let entry = &self.top.indexes[number];
        let available = allowance
            .checked_sub(self.metadata_bytes())
            .ok_or_else(|| cap("index_metadata"))?;
        let payload = self.diagnostic_frame(
            entry.offset,
            entry.len,
            crate::disk_run::INDEX_COUNTER_BASE + entry.number,
            self.block_limit(),
            "index_frame",
        )?;
        let mut at = 0;
        let blocks = admitted(
            crate::disk_run::footer::get_refs_bounded(&payload, &mut at, available),
            "index_metadata",
        )?;
        if blocks.first().map(|block| &block.first_key) != Some(&entry.first_key) {
            return Err(corrupt("diagnostic index first key"));
        }
        let filter_start = at;
        crate::disk_run::bloom::Filter::read(&payload, &mut at)?;
        if at != payload.len() {
            return Err(corrupt("diagnostic index trailing bytes"));
        }
        let allocated = blocks.capacity() * std::mem::size_of::<BlockRef>()
            + blocks
                .iter()
                .map(|block| block.first_key.capacity())
                .sum::<usize>();
        if (payload.capacity() + allocated + payload.len() - filter_start) as u64 > available {
            return Err(cap("index_metadata"));
        }
        Ok(super::super::IndexBlock {
            blocks,
            filter_bytes: payload[filter_start..].to_vec(),
        })
    }
}
