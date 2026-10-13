/// Default ceiling on the on-disk bytes of the planes a personal
/// `Snapshot::create` photograph reads.
///
/// That photograph is not streamed: it holds every row of every photographed
/// plane as base64 strings in one `Vec`, then `seal()` builds the JSON and the
/// ciphertext of the whole thing. Peak memory is a multiple of the store, not
/// of one namespace. On Tom's primary (2026-09-29, ~12.6 GiB of photographed
/// planes on disk) every attempt pushed the footprint from ~9.5 GiB to 14-17 GiB
/// against a 16 GiB memory-guard ceiling — and those attempts were cut short by
/// two unreadable `cas_blobs` rows; a full pass cannot fit at all. It retried
/// every ~22 min for days with no completion.
///
/// Above the cap the attempt is refused before any row is read. Personal data
/// stays protected by the chunked backup (`sync::backup`), which never holds
/// the store in one buffer. What stops is only the remote personal log prune.
pub(crate) const PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES_DEFAULT: u64 = 1024 * 1024 * 1024;

/// The cap in force for this process, from
/// `LASTDB_PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES`. `0` removes the cap (returns
/// `None`); unset or unparseable falls back to the default.
pub(crate) fn personal_photograph_max_disk_bytes() -> Option<u64> {
    parse_personal_photograph_max_disk_bytes(
        std::env::var("LASTDB_PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES")
            .ok()
            .as_deref(),
    )
}

/// Pure half of [`personal_photograph_max_disk_bytes`], so tests do not mutate
/// a process-global env var that concurrent compaction tests would read.
pub(crate) fn parse_personal_photograph_max_disk_bytes(raw: Option<&str>) -> Option<u64> {
    match raw.and_then(|raw| raw.trim().parse::<u64>().ok()) {
        Some(0) => None,
        Some(max) => Some(max),
        None => Some(PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES_DEFAULT),
    }
}

/// Refusal message when the photographed planes exceed the cap, else `None`.
///
/// `disk_bytes: None` means the backend cannot measure (in-memory stores,
/// tests); that keeps today's behaviour rather than guessing.
pub(crate) fn personal_photograph_size_refusal(
    disk_bytes: Option<u64>,
    max_bytes: Option<u64>,
) -> Option<String> {
    let (disk_bytes, max_bytes) = (disk_bytes?, max_bytes?);
    (disk_bytes > max_bytes).then(|| {
        format!(
            "personal photograph refused: the photographed planes hold {disk_bytes} bytes on \
             disk, over the {max_bytes}-byte cap for an in-memory photograph \
             (LASTDB_PERSONAL_PHOTOGRAPH_MAX_DISK_BYTES; 0 removes the cap). The chunked \
             backup is unaffected; only the remote personal log prune is skipped"
        )
    })
}
