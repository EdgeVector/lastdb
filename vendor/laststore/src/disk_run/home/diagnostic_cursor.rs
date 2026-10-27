//! Checkpoint merge inputs: tiered runs separately, one sequential base input.
use super::manifest::RunFile;
use super::RunHome;
use crate::disk_run::reader::stream::SummaryCursor;
use crate::{Error, Result};
use std::sync::Arc;
use std::time::Instant;

type InventoryRecord = (Vec<u8>, Option<u64>);

pub(super) enum InventoryCursor<'a> {
    Run(SummaryCursor),
    Base {
        home: &'a RunHome,
        files: &'a [Arc<RunFile>],
        next: usize,
        current: Option<SummaryCursor>,
        allowance: u64,
    },
}

impl<'a> InventoryCursor<'a> {
    pub(super) fn run(home: &RunHome, run: &RunFile, allowance: u64) -> Result<Self> {
        Ok(Self::Run(SummaryCursor::open(
            &run.path, &home.key, allowance,
        )?))
    }

    pub(super) fn base(home: &'a RunHome, files: &'a [Arc<RunFile>], allowance: u64) -> Self {
        Self::Base {
            home,
            files,
            next: 0,
            current: None,
            allowance,
        }
    }

    pub(super) fn next_record(&mut self, deadline: Instant) -> Result<Option<InventoryRecord>> {
        match self {
            Self::Run(cursor) => cursor.next_record(),
            Self::Base {
                home,
                files,
                next,
                current,
                allowance,
            } => loop {
                if Instant::now() >= deadline {
                    return Err(Error::ReadBudgetExceeded);
                }
                if let Some(cursor) = current {
                    if let Some(record) = cursor.next_record()? {
                        return Ok(Some(record));
                    }
                }
                // Drop the old reader and buffers before admitting the next file.
                *current = None;
                let Some(run) = files.get(*next) else {
                    return Ok(None);
                };
                *current = Some(SummaryCursor::open(&run.path, &home.key, *allowance)?);
                *next += 1;
            },
        }
    }
}
