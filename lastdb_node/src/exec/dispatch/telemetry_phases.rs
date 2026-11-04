use crate::request_telemetry::PhaseTimings;

/// Per-request phase timings for one telemetry sample: the socket queue wait
/// plus the totals the phase accumulator gathered while the route ran.
pub(super) fn phase_timings(
    queue_wait_us: u64,
    phase_totals: &fold_db::request_phases::RequestPhaseTotals,
) -> PhaseTimings {
    PhaseTimings {
        queue_wait_us,
        admission_wait_us: phase_totals.admission_wait_us,
        parse_us: phase_totals.parse_us,
        schema_resolve_us: phase_totals.schema_resolve_us,
        validate_us: phase_totals.validate_us,
        cas_precondition_us: phase_totals.cas_precondition_us,
        count_us: phase_totals.count_us,
        hydrate_us: phase_totals.hydrate_us,
        hydrate_records_us: phase_totals.hydrate_records_us,
        hydrate_format_us: phase_totals.hydrate_format_us,
        hydrate_sort_us: phase_totals.hydrate_sort_us,
        apply_us: phase_totals.apply_us,
        grouping_us: phase_totals.grouping_us,
        index_wait_us: phase_totals.index_wait_us,
        change_record_us: phase_totals.change_record_us,
        response_envelope_us: phase_totals.response_envelope_us,
        status_durability_us: phase_totals.status_durability_us,
        status_data_dir_us: phase_totals.status_data_dir_us,
        status_request_ops_us: phase_totals.status_request_ops_us,
    }
}
