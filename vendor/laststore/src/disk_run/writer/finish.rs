//! Complete run bytes separately from their durable namespace publication.

use super::*;
use crate::disk_run::footer::{Footer, Top, FOOTER_BYTES};
use crate::disk_run::{MAX_TOP_BYTES, RUN_SUFFIX, TOP_COUNTER};
use crate::durability::{sync_directory, sync_dirty_file};

/// Not readable or publishable until finish returns successfully. Its writer
/// retains the temporary-file cleanup and any merge-space reservation.
pub(crate) struct PreparedRun {
    writer: RunWriter,
    summary: RunSummary,
}

impl PreparedRun {
    pub(crate) fn summary(&self) -> &RunSummary {
        &self.summary
    }

    pub(crate) fn finish(mut self) -> Result<RunSummary> {
        let _durable = timing::stage("v2_run_durable_us");
        timing::count("v2_run_file_flushes", 1);
        timing::during("v2_run_file_flush_us", || {
            sync_dirty_file(&self.writer.file)
        })?;
        let path = &self.summary.path;
        timing::during("v2_run_rename_us", || fs::rename(&self.writer.tmp, path))?;
        if let Some(space) = &self.writer.space {
            space.output(path);
        }
        timing::count("v2_run_directory_flushes", 1);
        timing::during("v2_run_directory_flush_us", || {
            sync_directory(&File::open(&self.writer.dir)?)
        })?;
        self.writer.finished = true;
        timing::count("v2_run_records", self.writer.records);
        timing::count("v2_run_tombstones", self.writer.tombstones);
        timing::count("v2_run_record_bytes", self.writer.record_bytes);
        Ok(self.summary)
    }
}

impl RunWriter {
    pub(crate) fn prepare_finish(mut self) -> Result<PreparedRun> {
        self.refuse_after_failure()?;
        if !self.block.is_empty() {
            self.flush_block()?;
        }
        if !self.index.is_empty() {
            self.flush_index()?;
        }
        let max_payload =
            u32::try_from(self.max_payload).map_err(|_| corrupt("block over 4 GiB"))?;
        let data_blocks =
            u32::try_from(self.data_blocks).map_err(|_| corrupt("too many data blocks"))?;
        let top = Top {
            records: self.records,
            data_blocks: self.data_blocks,
            max_payload: u64::from(max_payload),
            indexes: std::mem::take(&mut self.top),
            last_key: self.last_key.clone().unwrap_or_default(),
        };
        let top_entries = top.indexes.len();
        let top = timing::during("v2_top_encoding_us", || top.encode());
        if top.len() > MAX_TOP_BYTES || top_entries > MAX_INDEX_ENTRIES {
            return Err(Error::WriteCapacity(
                "disk run: top block over its read limit; write smaller runs".into(),
            ));
        }
        let (top_offset, top_len) = self.write_frame(TOP_COUNTER, &top)?;
        timing::count("v2_run_top_blocks", 1);
        let footer = Footer {
            run_id: self.run_id,
            top_offset,
            top_len,
            records: self.records,
            max_payload,
            data_blocks,
        };
        if let Some(space) = &self.space {
            space.admit_run(FOOTER_BYTES as u64)?;
        }
        timing::during("v2_run_write_us", || self.file.write_all(&footer.encode()))?;
        if let Some(space) = &self.space {
            space.materialize(FOOTER_BYTES as u64)?;
        }
        timing::count("v2_run_written_bytes", FOOTER_BYTES as u64);
        let summary = RunSummary {
            path: self.dir.join(format!("{}{RUN_SUFFIX}", self.run_id)),
            run_id: self.run_id,
            records: self.records,
            tombstones: self.tombstones,
            record_bytes: self.record_bytes,
            tombstone_bytes: self.tombstone_bytes,
            file_bytes: self.offset + FOOTER_BYTES as u64,
            first_key: self.first_key.take(),
            last_key: self.last_key.take(),
        };
        Ok(PreparedRun {
            writer: self,
            summary,
        })
    }
}
