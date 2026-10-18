//! Record-count admission bound shared by the native core model.

/// Used-record cap for the native core model.
///
/// Decision `decision-2026-10-02-warm-set-exact-logical-key-budget`. One
/// fetched schema, field, molecule tip, tombstone, or atom is one used
/// record. The budget is this count, not a byte ledger.
pub const RESIDENT_KEY_CAP: usize = 10_000;

/// Optional lower operational cap. Zero and values above 10,000 are refused.
pub const RESIDENT_KEY_CAP_ENV: &str = "LASTDB_RESIDENT_KEY_CAP";

static RESIDENT_KEY_CAP_IN_FORCE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Parse the cap override. `None` (unset) is [`RESIDENT_KEY_CAP`].
pub fn parse_resident_key_cap(raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(RESIDENT_KEY_CAP);
    };
    let cap = raw
        .trim()
        .parse::<usize>()
        .map_err(|error| format!("invalid {RESIDENT_KEY_CAP_ENV}={raw:?}: {error}"))?;
    if !(1..=RESIDENT_KEY_CAP).contains(&cap) {
        return Err(format!(
            "{RESIDENT_KEY_CAP_ENV}={cap} is outside [1, {RESIDENT_KEY_CAP}] (it only lowers the cap)"
        ));
    }
    Ok(cap)
}

/// Read [`RESIDENT_KEY_CAP_ENV`] once, at boot. The daemon refuses to start
/// on a bad value.
pub fn init_resident_key_cap_from_env() -> Result<usize, String> {
    let cap = parse_resident_key_cap(std::env::var(RESIDENT_KEY_CAP_ENV).ok().as_deref())?;
    Ok(*RESIDENT_KEY_CAP_IN_FORCE.get_or_init(|| cap))
}

/// The used-record cap in force: [`RESIDENT_KEY_CAP`] until boot ran
/// [`init_resident_key_cap_from_env`] with an override.
#[must_use]
pub fn resident_key_cap() -> usize {
    RESIDENT_KEY_CAP_IN_FORCE
        .get()
        .copied()
        .unwrap_or(RESIDENT_KEY_CAP)
}
