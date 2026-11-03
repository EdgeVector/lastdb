//! Per-request microsecond totals, one field per [`super::RequestPhase`].

/// Accumulated per-phase totals for one request, in microseconds.
///
/// Fields are microseconds because several phases are sub-millisecond and
/// would read as constant zeroes in ms. Zero means "not reported": every
/// consumer treats an all-zero set as absent, so recording sites never need
/// a "was anything measured" side channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestPhaseTotals {
    pub admission_wait_us: u64,
    pub parse_us: u64,
    pub schema_resolve_us: u64,
    pub validate_us: u64,
    pub cas_precondition_us: u64,
    pub count_us: u64,
    pub hydrate_us: u64,
    pub hydrate_records_us: u64,
    pub hydrate_format_us: u64,
    pub hydrate_sort_us: u64,
    pub apply_us: u64,
    pub grouping_us: u64,
    pub idempotency_check_us: u64,
    pub index_wait_us: u64,
    pub change_record_us: u64,
    pub response_envelope_us: u64,
    pub status_durability_us: u64,
    pub status_data_dir_us: u64,
    pub status_request_ops_us: u64,
}
