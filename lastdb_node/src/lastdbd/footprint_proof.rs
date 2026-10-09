//! Debug-only allocation pin used by the CoW footprint proof.

const FOOTPRINT_PROOF_PIN_BYTES_ENV: &str = "LASTDB_FOOTPRINT_PROOF_PIN_BYTES";

pub(crate) fn parse_footprint_proof_pin_bytes(
    raw: Option<&str>,
    keychain_disabled: bool,
) -> Result<Option<u64>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let requested = raw
        .trim()
        .parse::<u64>()
        .map_err(|error| format!("invalid {FOOTPRINT_PROOF_PIN_BYTES_ENV}={raw:?}: {error}"))?;
    if requested == 0 {
        return Ok(None);
    }
    if !keychain_disabled {
        return Err(format!(
            "{FOOTPRINT_PROOF_PIN_BYTES_ENV} requires FOLDDB_DISABLE_KEYCHAIN=1"
        ));
    }
    Ok(Some(requested))
}

/// Hold a debug-only live allocation for the CoW footprint proof. Release
/// builds reject the knob, and debug builds require keychain-disabled mode so
/// the fixture cannot attach itself to a normal service boot by accident.
pub(crate) fn footprint_proof_pin_from_env() -> Result<Option<Box<[u8]>>, String> {
    let raw = std::env::var(FOOTPRINT_PROOF_PIN_BYTES_ENV).ok();
    let keychain_disabled = std::env::var("FOLDDB_DISABLE_KEYCHAIN").ok().as_deref() == Some("1");
    let Some(requested) = parse_footprint_proof_pin_bytes(raw.as_deref(), keychain_disabled)?
    else {
        return Ok(None);
    };
    #[cfg(not(debug_assertions))]
    {
        let _ = requested;
        Err(format!(
            "{FOOTPRINT_PROOF_PIN_BYTES_ENV} is available only in debug builds"
        ))
    }
    #[cfg(debug_assertions)]
    {
        let bytes = usize::try_from(requested).map_err(|_| {
            format!("{FOOTPRINT_PROOF_PIN_BYTES_ENV} exceeds this platform's address space")
        })?;
        let mut allocation = Vec::new();
        allocation.try_reserve_exact(bytes).map_err(|error| {
            format!("could not reserve {requested} footprint-proof bytes: {error}")
        })?;
        allocation.resize(bytes, 0xA5);
        tracing::warn!(
            bytes = requested,
            "holding debug-only non-reclaimable allocation for the footprint proof"
        );
        Ok(Some(allocation.into_boxed_slice()))
    }
}
