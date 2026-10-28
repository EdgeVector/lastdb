use super::model::hex;

pub(super) const STATE_KEY: &[u8] = b"state";

pub(super) fn data_prefix(hash: &str) -> Vec<u8> {
    format!("d\0{}\0", hex(hash.as_bytes())).into_bytes()
}

pub(super) fn data_key(hash: &str, range: &str) -> Vec<u8> {
    let mut key = data_prefix(hash);
    key.extend_from_slice(range.as_bytes());
    key
}

pub(super) fn event_key(origin: &str, sequence: u64) -> Vec<u8> {
    format!("e\0{origin}\0{sequence:020}").into_bytes()
}

pub(super) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for byte in end.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return end;
        }
        *byte = 0;
    }
    unreachable!("the spike prefixes contain an ASCII domain")
}
