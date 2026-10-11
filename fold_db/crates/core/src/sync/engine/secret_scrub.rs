//! End the cached encryption key lifetime with the engine.

use super::SyncEngine;

impl Drop for SyncEngine {
    fn drop(&mut self) {
        // Scrub the cached at-rest key from memory on drop (Gap G3).
        use zeroize::Zeroize as _;
        if let Some(key) = self.enc_key.as_mut() {
            key.zeroize();
        }
    }
}
