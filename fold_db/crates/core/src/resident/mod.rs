mod config;
pub(crate) mod core_keys;
mod core_set;
mod lane;
mod metrics;
mod warm_bytes;

pub use config::{
    parse_resident_max_deferred, ResidentPolicy, DEFAULT_RESIDENT_MAX_DEFERRED,
    RESIDENT_MAX_DEFERRED_ENV,
};
pub(crate) use core_set::CoreMemorySource;
pub use core_set::{CoreMemoryStats, CoreMemoryValue, CoreWarmByteStats};
pub(crate) use core_set::{CoreModelChange, CoreModelStats, CoreModelUpdate, PreparedCoreMemory};
pub(crate) use core_set::{
    CorePath, CoreWarmLookup, CoreWarmSet, CoreWarmSlot, CoreWarmTip, RecordCoordinate,
};
pub use lane::{
    PersistBatchOutcome, PersistEnvelope, PersistLaneFull, PersistLaneKey, PersistLaneOccupancy,
    PersistLanePressure, PersistLaneSet, PersistLaneWriter, PersistReservation,
};
pub use metrics::{PersistLaneMetrics, PersistLaneMetricsSnapshot};
pub use warm_bytes::{
    core_warm_bytes, core_warm_dirty_bytes, init_core_warm_bytes_from_env, parse_core_warm_bytes,
    CORE_WARM_BYTES_ENV, CORE_WARM_DIRTY_PERCENT, DEFAULT_CORE_WARM_BYTES,
};
