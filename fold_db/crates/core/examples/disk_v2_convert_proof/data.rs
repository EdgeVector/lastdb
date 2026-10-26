//! The synthetic data of the V4 conversion proof: the write batches of the
//! v1 and v2 phases, and the expected records after each phase.

use super::*;

pub(super) enum Batch {
    Put(Vec<(Vec<u8>, Vec<u8>)>),
    Delete(Vec<Vec<u8>>),
}

pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xd1b5_4a32_d192_ed03)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub(super) fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

pub(super) fn key(n: usize) -> Vec<u8> {
    format!("k{n:06}").into_bytes()
}

/// Write, overwrite, and delete on the v1 home.
pub(super) fn v1_batches() -> Vec<Batch> {
    let mut rng = Rng::new(1);
    let mut batches = Vec::new();
    for chunk in (0..RECORDS).collect::<Vec<_>>().chunks(BATCH) {
        batches.push(Batch::Put(
            chunk
                .iter()
                .map(|&n| (key(n), rng.bytes(VALUE_BYTES)))
                .collect(),
        ));
    }
    for chunk in (0..RECORDS).step_by(5).collect::<Vec<_>>().chunks(BATCH) {
        batches.push(Batch::Put(
            chunk
                .iter()
                .map(|&n| (key(n), rng.bytes(VALUE_BYTES)))
                .collect(),
        ));
    }
    let deleted: Vec<_> = (3..RECORDS).step_by(10).map(key).collect();
    batches.extend(deleted.chunks(BATCH).map(|c| Batch::Delete(c.to_vec())));
    batches
}

/// New writes and deletes on the converted home.
pub(super) fn v2_batches() -> Vec<Batch> {
    let mut rng = Rng::new(2);
    let items: Vec<_> = (0..RECORDS)
        .step_by(7)
        .map(|n| (key(n), rng.bytes(VALUE_BYTES)))
        .collect();
    let mut batches: Vec<_> = items
        .chunks(BATCH)
        .map(|c| Batch::Put(c.to_vec()))
        .collect();
    let deleted: Vec<_> = (1..RECORDS).step_by(11).map(key).collect();
    batches.extend(deleted.chunks(BATCH).map(|c| Batch::Delete(c.to_vec())));
    batches
}

/// CAS files on the v1 home.
pub(super) fn cas_v1_batches() -> Vec<Batch> {
    let mut rng = Rng::new(3);
    let items: Vec<_> = (0..CAS_RECORDS)
        .map(|n| (cas_key(n), rng.bytes(CAS_BYTES)))
        .collect();
    items.chunks(50).map(|c| Batch::Put(c.to_vec())).collect()
}

/// New CAS files and CAS deletes on the converted home, in one batch each
/// way: the commit splits them across the two run homes.
pub(super) fn cas_v2_batches() -> Vec<Batch> {
    let mut rng = Rng::new(4);
    let items: Vec<_> = (CAS_RECORDS..CAS_RECORDS + 50)
        .map(|n| (cas_key(n), rng.bytes(CAS_BYTES)))
        .collect();
    vec![
        Batch::Put(items),
        Batch::Delete((0..CAS_RECORDS).step_by(6).map(cas_key).collect()),
    ]
}

pub(super) fn cas_key(n: usize) -> Vec<u8> {
    format!("blob{n:05}").into_bytes()
}

pub(super) fn expected_cas_v1() -> Records {
    let mut expected = BTreeMap::new();
    apply(&mut expected, cas_v1_batches());
    expected
}

pub(super) fn expected_cas_v2() -> Records {
    let mut expected = expected_cas_v1();
    apply(&mut expected, cas_v2_batches());
    expected
}

pub(super) fn apply(expected: &mut Records, batches: Vec<Batch>) {
    for batch in batches {
        match batch {
            Batch::Put(items) => expected.extend(items),
            Batch::Delete(keys) => {
                for key in keys {
                    expected.remove(&key);
                }
            }
        }
    }
}

pub(super) fn expected_v1() -> Records {
    let mut expected = BTreeMap::new();
    apply(&mut expected, v1_batches());
    expected
}

pub(super) fn expected_v2() -> Records {
    let mut expected = expected_v1();
    apply(&mut expected, v2_batches());
    expected
}
