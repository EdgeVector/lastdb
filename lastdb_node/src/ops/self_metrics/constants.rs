use super::*;

pub const TELEMETRY_NAMESPACE: &str = "lastdb_telemetry";
pub const SELF_METRIC_SCHEMA: &str = "lastdb_telemetry/SelfMetricSample";
pub const REQUEST_OPS_ROLLUP_SCHEMA: &str = "lastdb_telemetry/RequestOpsRollup";
pub const SELF_METRIC_SERIES: &str = "lastdbd-self";
pub const REQUEST_OPS_ROLLUP_SERIES: &str = "request-ops";
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);
/// Maximum wall time the background sampler waits for a fresh sync snapshot.
/// `/api/status` never performs this collection; it reads the published value.
pub(super) const SAMPLER_SYNC_SNAPSHOT_BUDGET: Duration = Duration::from_millis(250);
/// How long a measured data-dir size is served before it is refreshed in the
/// background. The walk is a display gauge, not a correctness input, so a
/// minute of staleness is cheaper than re-walking the store per request.
pub const DEFAULT_DATA_DIR_SIZE_TTL: Duration = Duration::from_secs(60);
/// Storage ceiling for the durable self-metric sample plane.
///
/// This plane used to be capped at `30 * 24 * 60` = 43,200 ROWS, read as "30
/// days of one sample a minute". That number bounded the window, not the
/// storage: a row costs `fields x tip_bytes`, and at today's 54 fields the
/// row cap put the plane's ceiling at **1.29 GB** — five times the
/// byte-budgeted sibling below, on the same store. Adding one observability
/// field grew that ceiling by ~24 MB silently. It is the same defect the
/// rollup budget fixed, one constant to the left; see
/// `papercut-lastdb-selfmetricsample-plane-still-row-capped-at-1-29-gb`.
///
/// The honest reading of the old constant: the 30-day window was never 30
/// days of *bounded* storage, it was 1.29 GB of storage that happened to be
/// 30 days wide at that day's field count.
///
/// 256 MiB derives to 8,956 rows at 54 fields, so the default window is
/// **~6.2 days** rather than 30. That is a deliberate trade, and it is
/// cheaper than it looks: this plane is off by default
/// (`LASTDB_SELF_METRICS_TO_DB`), and the same vitals are written
/// out-of-band to `logs/self-metrics.jsonl`, which is bounded at 64 MiB and
/// survives the node being wedged — so the long-horizon debugging record
/// does not live here. Override with `LASTDB_SELF_METRICS_MAX_BYTES`, or set
/// an explicit row cap with `LASTDB_SELF_METRICS_MAX_SAMPLES` to bypass the
/// derivation entirely.
pub const DEFAULT_SELF_METRIC_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Storage ceiling for the durable request-ops rollup plane.
///
/// The budget is stated in BYTES because bytes are what grows. The retention
/// cap used to be a row count (`48 * 60 * 64` = 184,320 rows, read as "48h of
/// 64 buckets a minute") while the cost of a row is
/// `fields x tip_bytes` — so the two moved independently, and when
/// `REQUEST_OPS_ROLLUP_FIELDS` went from 12 to 39 the plane's ceiling tripled
/// to ~7.19M tips (~3.95 GB) with no edit to the constant that was supposed
/// to bound it, on a 7.25 GiB store. A row count cannot bound a plane whose
/// per-row cost is a free variable.
///
/// 256 MiB is a deliberate cut: telemetry about the database is not entitled
/// to a multi-gigabyte share of the user's database. Override with
/// `LASTDB_REQUEST_OPS_ROLLUP_MAX_BYTES`, or set an explicit row cap with
/// `LASTDB_REQUEST_OPS_ROLLUP_MAX_ROWS` to bypass the derivation entirely.
pub const DEFAULT_REQUEST_OPS_ROLLUP_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// What ALL durable telemetry is allowed to cost, in total, by default.
///
/// Both papercuts in this area were per-plane: each plane's cap was reviewed
/// against itself, and nothing anywhere stated what telemetry costs *added
/// up*. Two planes at 256 MiB each is 512 MiB, and that number had never been
/// written down or agreed to — it was the arithmetic consequence of two
/// independent decisions.
///
/// This constant is the place that number lives. It is enforced by
/// `telemetry_plane_budgets_stay_within_the_total`, which sums
/// [`TELEMETRY_PLANE_BUDGETS`] and fails if a new plane, or a raised budget,
/// pushes the total past it. That test is the point of the constant: it makes
/// the aggregate a decision someone has to make on purpose, instead of a sum
/// nobody computes.
pub const DEFAULT_TELEMETRY_TOTAL_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Every durable telemetry plane, with the byte budget that bounds it and the
/// per-row field count that budget is divided by.
///
/// This table exists to be asserted over, not to be read at runtime — a plane
/// that is missing from it is a plane whose ceiling is in nobody's total. Add
/// a row here when you add a telemetry plane; the guards below will tell you
/// what it costs.
pub const TELEMETRY_PLANE_BUDGETS: &[(&str, u64, usize)] = &[
    (
        SELF_METRIC_SCHEMA,
        DEFAULT_SELF_METRIC_MAX_BYTES,
        SELF_METRIC_FIELDS.len(),
    ),
    (
        REQUEST_OPS_ROLLUP_SCHEMA,
        DEFAULT_REQUEST_OPS_ROLLUP_MAX_BYTES,
        REQUEST_OPS_ROLLUP_FIELDS.len(),
    ),
];

/// Measured cost of one tip plus its atom on the primary, in bytes.
///
/// From `lastdb db inventory` on the live primary 2026-08-03: the `mk:` tip
/// plane was 1,134,259,272 B over 2,044,361 keys = 555 B/key. Used only to
/// turn the byte budget into a row cap, so it wants to be a defensible
/// order-of-magnitude constant, not a precise one.
pub const ROLLUP_TIP_BYTES: u64 = 555;
/// Relative path under node home for the out-of-band vitals log (JSONL).
/// Survives LastDB being wedged — unlike SelfMetricSample mutations.
pub const DEFAULT_SELF_METRICS_LOG_REL: &str = "logs/self-metrics.jsonl";
/// Rotate the JSONL file when it exceeds this many bytes (default 64 MiB).
pub const DEFAULT_SELF_METRICS_LOG_MAX_BYTES: u64 = 64 * 1024 * 1024;
