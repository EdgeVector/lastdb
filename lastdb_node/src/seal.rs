//! HKDF-SHA256 sealed-box key derivation for share invitations and contributions.

use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::SharedSecret;
use zeroize::Zeroizing;

pub(crate) fn sealed_box_key(
    shared: &SharedSecret,
    context: &[u8],
) -> Result<Zeroizing<[u8; 32]>, &'static str> {
    if !shared.was_contributory() {
        return Err("non-contributory X25519 key");
    }
    let hk = Hkdf::<Sha256>::new(None, shared.as_bytes());
    let mut key = Zeroizing::new([0u8; 32]);
    hk.expand(context, &mut *key)
        .map_err(|_| "sealed box key derivation failed")?;
    Ok(key)
}
