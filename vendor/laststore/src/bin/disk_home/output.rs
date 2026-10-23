//! Machine-readable command output has an explicit public field boundary.
//! Storage identities and opaque internal error strings are never serialized.
//! Requested keys and stored bodies remain intentional read/range results.

use laststore::disk_pack::{home::MutationQuote, ReadCosts};
use laststore::DiskReadReceipt;
use serde::Serialize;
use std::io::{self, Write};

/// Stderr carries a bounded category, never an internal error's opaque text.
pub(super) fn command_error(error: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(error) = error.downcast_ref::<laststore::Error>() {
        return match error {
            laststore::Error::Io(error) => io_error(error),
            laststore::Error::Corrupt(detail)
                if detail.ends_with("unsupported CURRENT feature") =>
            {
                "unsupported CURRENT feature"
            }
            laststore::Error::Corrupt(_) => "corrupt native storage",
            laststore::Error::AeadAuthFail => "storage authentication failed",
            laststore::Error::ChunkQuarantined { .. } => "storage was quarantined",
            laststore::Error::Config(detail)
                if detail.starts_with("unsupported internal-journal disk format:") =>
            {
                "unsupported internal-journal disk format; explicit snapshot conversion required"
            }
            laststore::Error::Config(_) => "unsupported storage configuration",
            laststore::Error::ReadBudgetExceeded => "disk-pack v1: read resource limit exceeded",
            laststore::Error::WriteCapacity(_) => "disk-pack v1: write capacity unavailable",
            laststore::Error::ColdGroupTooLarge { .. } => "storage group exceeds its size limit",
            laststore::Error::UnanchoredRead { .. } => "read requires a complete hash",
        };
    }
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return io_error(error);
    }
    if error.is::<serde_json::Error>() {
        return "invalid JSON input";
    }
    if error.is::<std::num::ParseIntError>() {
        return "invalid numeric argument";
    }
    "command failed"
}

fn io_error(error: &io::Error) -> &'static str {
    match error.kind() {
        io::ErrorKind::InvalidInput => "invalid command input; use --help for the input contract",
        io::ErrorKind::NotFound => "required file was not found",
        io::ErrorKind::PermissionDenied => "file access was denied",
        io::ErrorKind::AlreadyExists => "destination already exists",
        io::ErrorKind::Interrupted => "I/O was interrupted",
        io::ErrorKind::BrokenPipe => "output pipe was closed",
        _ => "I/O failed",
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum CommandOutput {
    Build {
        built: bool,
        records: u64,
        generation: u64,
        frontier: u64,
        required_features: u64,
        hash_fanout: u16,
        placement_codec: u16,
        key_codec: u16,
    },
    Plan {
        snapshot_quote: SnapshotQuote,
        operations: usize,
        disk_reserved: bool,
        logical_records_reserved: bool,
        memory_ack_allowed: bool,
    },
    Write {
        previous_frontier: u64,
        frontier: u64,
        generation: u64,
        operations: usize,
        snapshot_quote: SnapshotQuote,
        published: bool,
    },
    Read {
        results: Vec<ReadResult>,
        generation: u64,
        frontier: u64,
        costs: Costs,
    },
    Range {
        pages: Vec<RangePage>,
        exhausted: bool,
        generation: u64,
        frontier: u64,
        costs: Costs,
    },
}

#[derive(Serialize)]
pub(super) struct SnapshotQuote {
    physical_bytes: u64,
    map_pages: u64,
    dependency_parts: u64,
    scope: &'static str,
}

impl From<MutationQuote> for SnapshotQuote {
    fn from(quote: MutationQuote) -> Self {
        Self {
            physical_bytes: quote.physical_bytes(),
            map_pages: quote.map_pages(),
            dependency_parts: quote.dependency_parts(),
            scope: "complete_snapshot",
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum ReadResult {
    Body {
        body_hex: Option<String>,
    },
    Error {
        error: &'static str,
        resource_exhausted: bool,
    },
}

impl ReadResult {
    pub(super) fn failed(error: &laststore::Error) -> Self {
        // Errors can carry storage UUIDs, paths, or caller-controlled details.
        // Keep their categories, never their opaque Display representation.
        let message = match error {
            laststore::Error::Io(_) => "native read I/O failed",
            laststore::Error::Corrupt(_) => "native read found corrupt storage",
            laststore::Error::AeadAuthFail => "native read authentication failed",
            laststore::Error::ChunkQuarantined { .. } => "native read found quarantined storage",
            laststore::Error::Config(_) => "native read configuration is unsupported",
            laststore::Error::ReadBudgetExceeded => "disk-pack v1: read resource limit exceeded",
            laststore::Error::WriteCapacity(_) => "native read lacks storage capacity",
            laststore::Error::ColdGroupTooLarge { .. } => {
                "native read exceeds its group size limit"
            }
            laststore::Error::UnanchoredRead { .. } => "native read requires a complete hash",
        };
        Self::Error {
            error: message,
            resource_exhausted: matches!(error, laststore::Error::ReadBudgetExceeded),
        }
    }
}

#[derive(Serialize)]
pub(super) struct RangePage {
    pub(super) exhausted: bool,
    pub(super) records: Vec<RangeRecord>,
}

#[derive(Serialize)]
pub(super) struct RangeRecord {
    pub(super) key: String,
    pub(super) body_hex: String,
    pub(super) batch: u64,
    pub(super) operation: u32,
}

#[derive(Serialize)]
pub(super) struct Costs {
    batch_read_runs: u64,
    file_handles_peak: u64,
    file_handle_reuses: u64,
    file_bytes: u64,
    budget_bytes: u64,
    directory_pages: u64,
    locator_pages: u64,
    map_pages: u64,
    overflow_pages: u64,
    payload_blocks: u64,
    shared_pages: u64,
    shared_blocks: u64,
    examined_keys: u64,
}

impl From<ReadCosts> for Costs {
    fn from(cost: ReadCosts) -> Self {
        Self {
            batch_read_runs: cost.batch_read_runs,
            file_handles_peak: cost.file_handles_peak,
            file_handle_reuses: cost.file_handle_reuses,
            file_bytes: cost.file_bytes,
            budget_bytes: cost.budget_bytes,
            directory_pages: cost.directory_pages,
            locator_pages: cost.locator_pages,
            map_pages: cost.map_pages,
            overflow_pages: cost.overflow_pages,
            payload_blocks: cost.payload_blocks,
            shared_pages: cost.shared_pages,
            shared_blocks: cost.shared_blocks,
            examined_keys: cost.examined_keys,
        }
    }
}

#[derive(Serialize)]
struct Response {
    #[serde(flatten)]
    result: CommandOutput,
    elapsed_nanos: u128,
    disk: DiskCosts,
}

#[derive(Serialize)]
struct DiskCosts {
    file_opens: u64,
    read_operations: u64,
    format_probe_bytes: u64,
    payload_bytes: u64,
    group_opens: u64,
    legacy_replay_bytes: u64,
    footer_bytes: u64,
    directory_reads: u64,
    temporary_bytes_peak: u64,
}

/// Export the explicit response schema through the command's data channel.
pub(super) fn write_response(
    result: CommandOutput,
    elapsed_nanos: u128,
    disk: DiskReadReceipt,
) -> super::Result<()> {
    let response = Response {
        result,
        elapsed_nanos,
        disk: DiskCosts {
            file_opens: disk.file_opens,
            read_operations: disk.read_operations,
            format_probe_bytes: disk.format_probe_bytes,
            payload_bytes: disk.payload_bytes,
            group_opens: disk.group_opens,
            legacy_replay_bytes: disk.legacy_replay_bytes,
            footer_bytes: disk.footer_bytes,
            directory_reads: disk.directory_reads,
            temporary_bytes_peak: disk.temporary_bytes_peak,
        },
    };
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, &response)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
