use super::*;

#[derive(Default)]
pub(super) struct HeaderVersions {
    pub(super) entries: BTreeMap<String, (String, u64)>,
    bytes: usize,
}

/// The digest binds this typed scalar to the exact staged encrypted bytes.
pub(crate) fn note_header_write(namespace: &str, key: &[u8], plain: &[u8], sealed: &[u8]) {
    if namespace != "main" {
        return;
    }
    let id = super::super::super::laststore::native_routing::encode(key);
    let Some(version) = super::super::validity::header_version(&id, plain) else {
        return;
    };
    let _ = WRITE.try_with(|pending| {
        let mut headers = pending
            .header_versions
            .lock()
            .expect("native staged header versions");
        let additional = if headers.entries.contains_key(&id) {
            0
        } else {
            id.len().saturating_add(192)
        };
        if (headers.entries.len() < 10_000 || additional == 0)
            && headers.bytes.saturating_add(additional) <= 4 * 1024 * 1024
        {
            headers.bytes += additional;
            headers
                .entries
                .insert(id, (crate::hex::sha256_hex(sealed), version));
        }
    });
}
