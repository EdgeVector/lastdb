use super::*;

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// v1 OPE range: 8 hex characters per plaintext byte (`core/crypto/e2e.rs`).
fn ope_v1(molecule: u32, range: &str) -> String {
    let mut out = String::with_capacity(range.len() * 8);
    for (i, b) in range.bytes().enumerate() {
        let d = Sha256::digest([molecule.to_le_bytes(), (i as u32).to_le_bytes()].concat());
        let word = (u32::from_be_bytes([d[0], d[1], d[2], d[3]]) & 0xFFFF_FF00) | u32::from(b);
        out.push_str(&format!("{word:08x}"));
    }
    out
}

fn group_of(prefix: &[u8]) -> u16 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in prefix {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h % GROUPS) as u16
}

fn v2_key(item: &Item, ope_range: bool) -> Vec<u8> {
    let mut key = Vec::with_capacity(48);
    let (class, prefix_len) = match item {
        Item::Body { id, .. } => {
            key.extend_from_slice(id);
            (1u8, 32)
        }
        Item::Extra { id } => {
            key.extend_from_slice(id);
            key.push(b'h');
            (1, 32)
        }
        Item::Tip {
            molecule,
            hash,
            range,
            ..
        } => {
            key.extend_from_slice(&molecule.to_be_bytes());
            key.extend_from_slice(hash);
            let prefix = key.len();
            if let Some(range) = range {
                if ope_range {
                    key.extend_from_slice(ope_v1(*molecule, range).as_bytes());
                } else {
                    key.extend_from_slice(range.as_bytes());
                }
            }
            (2, prefix)
        }
        Item::Idem { id } => {
            key.extend_from_slice(id);
            (3, 32)
        }
        Item::Event { seq, .. } => {
            key.extend_from_slice(&seq.to_be_bytes());
            (4, 0)
        }
        Item::Counter { id, .. } => {
            key.extend_from_slice(id);
            key.push(b'c');
            (1, 32)
        }
    };
    let group = group_of(&key[..prefix_len]);
    let mut out = Vec::with_capacity(key.len() + 3);
    out.push(class);
    out.extend_from_slice(&group.to_be_bytes());
    out.extend_from_slice(&key);
    out
}

fn v2_value(item: &Item) -> Vec<u8> {
    let mut out = Vec::new();
    match item {
        Item::Body {
            schema,
            field,
            content,
            ..
        } => {
            varint(&mut out, u64::from(*schema));
            varint(&mut out, u64::from(*field));
            varint(&mut out, 1_792_802_400); // created_at, seconds
            varint(&mut out, 3); // writer number in the home's writer table
            out.extend_from_slice(content);
        }
        Item::Counter { bytes, .. } => varint(&mut out, u64::from(*bytes)),
        Item::Extra { id } => out.extend_from_slice(id),
        Item::Tip { atom, .. } => {
            out.extend_from_slice(atom);
            varint(&mut out, 1);
        }
        Item::Idem { id } => {
            out.extend_from_slice(&id[..16]);
            varint(&mut out, 200);
        }
        Item::Event { atom, .. } => {
            out.extend_from_slice(atom);
            varint(&mut out, 1_792_802_400);
        }
    }
    out
}

pub fn v2_records_in_write_order(stream: &Stream, ope_range: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let events = stream
        .items
        .iter()
        .filter(|i| matches!(i, Item::Event { .. }))
        .count() as u64;
    let mut idem = 0u64;
    stream
        .items
        .iter()
        .filter(|item| match item {
            Item::Counter { .. } => false,
            Item::Idem { .. } => {
                idem += 1;
                idem % 500 == 0
            }
            Item::Event { seq, .. } => *seq + 10_000 >= events,
            _ => true,
        })
        .map(|item| (v2_key(item, ope_range), v2_value(item)))
        .collect()
}
