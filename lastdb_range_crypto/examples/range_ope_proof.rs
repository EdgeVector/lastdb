//! The single CI-only crypto proof authorized by Tom on 2026-10-24.
//! Generated seeds and values are disposable; no database or real key is read.

use lastdb_range_crypto::{term_bytes, Error, RangeOpeKey, RangeOpeRoot, RangeScope, RangeTerm};

const LIMIT: usize = 1 << 20;
const SCOPE: RangeScope<'static> = RangeScope {
    molecule: "generated-proof-molecule",
    blinded_hash: "generated-proof-blinded-hash",
    epoch: 7,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = RangeOpeRoot::from_private_key_seed(&[0x42; 32])?;
    let key = root.for_scope(SCOPE)?;
    let values = generated_values();
    let terms = values
        .iter()
        .map(|value| key.encrypt(value, LIMIT))
        .collect::<Result<Vec<_>, _>>()?;
    check_order(&key, &values, &terms)?;
    let intervals = check_prefixes(&key, &values, &terms)?;
    check_scopes(&root, &key)?;
    check_codec(&key, &values, &terms)?;
    println!(
        "range_ope_proof: passed; values={}; prefix_intervals={intervals}; scopes=5; codec=passed",
        values.len()
    );
    Ok(())
}

fn generated_values() -> Vec<Vec<u8>> {
    let mut values = vec![Vec::new(), vec![0], vec![255], vec![0, 0], vec![255, 255]];
    let mut state = 0x879a_4697_124f_5b09u64;
    for number in 0..2048 {
        let length = number % 65;
        let value = (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        values.push(value.clone());
        values.push([value.as_slice(), &[0]].concat());
        values.push([value.as_slice(), &[255]].concat());
    }
    for byte in 0..=255 {
        values.push(vec![byte]);
        values.push(vec![0x12, 0x34, byte]);
    }
    for length in [8, 32, 256, 4096, 8192] {
        values.push(vec![0; length]);
        values.push(vec![255; length]);
    }
    values.sort();
    values.dedup();
    values
}

fn check_order(key: &RangeOpeKey, values: &[Vec<u8>], terms: &[RangeTerm]) -> Result<(), Error> {
    for pair in terms.windows(2) {
        assert!(pair[0] < pair[1], "adjacent plaintext order changed");
    }
    for (value, term) in values.iter().zip(terms) {
        assert_eq!(&key.encrypt(value, LIMIT)?, term, "determinism changed");
    }
    for position in 0..values.len() {
        let other = (position * 997 + 31) % values.len();
        assert_eq!(
            values[position].cmp(&values[other]),
            terms[position].cmp(&terms[other])
        );
    }
    Ok(())
}

fn check_prefixes(
    key: &RangeOpeKey,
    values: &[Vec<u8>],
    terms: &[RangeTerm],
) -> Result<usize, Error> {
    let mut prefixes = vec![vec![], vec![255], vec![255, 255], vec![0], vec![0x12, 0x34]];
    for value in values.iter().step_by(79) {
        prefixes.push(value.clone());
        prefixes.push(value[..value.len() / 2].to_vec());
    }
    prefixes.sort();
    prefixes.dedup();
    for prefix in &prefixes {
        let bounds = key.encrypt_prefix(prefix, LIMIT)?;
        assert_eq!(bounds.end.is_none(), prefix.iter().all(|byte| *byte == 255));
        for (value, term) in values.iter().zip(terms) {
            let inside =
                term >= &bounds.start && bounds.end.as_ref().is_none_or(|upper| term < upper);
            assert_eq!(
                inside,
                value.starts_with(prefix),
                "prefix interval changed membership"
            );
        }
    }
    Ok(prefixes.len())
}

fn check_scopes(root: &RangeOpeRoot, key: &RangeOpeKey) -> Result<(), Error> {
    let value = b"generated-scope-value";
    let baseline = key.encrypt(value, LIMIT)?;
    let same = RangeOpeRoot::from_private_key_seed(&[0x42; 32])?;
    assert_eq!(baseline, same.for_scope(SCOPE)?.encrypt(value, LIMIT)?);
    let other = RangeOpeRoot::from_private_key_seed(&[0x43; 32])?;
    assert_ne!(baseline, other.for_scope(SCOPE)?.encrypt(value, LIMIT)?);
    for scope in [
        RangeScope {
            molecule: "another-molecule",
            ..SCOPE
        },
        RangeScope {
            blinded_hash: "another-hash",
            ..SCOPE
        },
        RangeScope { epoch: 8, ..SCOPE },
    ] {
        assert_ne!(baseline, root.for_scope(scope)?.encrypt(value, LIMIT)?);
    }
    assert!(matches!(
        root.for_scope(RangeScope {
            molecule: "",
            ..SCOPE
        }),
        Err(Error::InvalidScope)
    ));
    let empty_hash = root.for_scope(RangeScope {
        blinded_hash: "",
        ..SCOPE
    })?;
    assert_ne!(baseline, empty_hash.encrypt(value, LIMIT)?);
    Ok(())
}

fn check_codec(key: &RangeOpeKey, values: &[Vec<u8>], terms: &[RangeTerm]) -> Result<(), Error> {
    let mut prior_text: Option<String> = None;
    for (value, term) in values.iter().zip(terms) {
        let size = term_bytes(value.len())?;
        assert_eq!(size, 8 * value.len() + 9);
        assert_eq!(term.as_ref().len(), size);
        let text = term.to_storage_text(size * 2)?;
        assert_eq!(text.len(), 16 * value.len() + 18);
        assert_eq!(&RangeTerm::from_storage_text(&text, size)?, term);
        assert_eq!(term.to_storage_text(size * 2 - 1), Err(Error::ByteLimit));
        assert_eq!(
            RangeTerm::from_storage_text(&text, size - 1),
            Err(Error::ByteLimit)
        );
        assert_eq!(key.encrypt(value, size - 1), Err(Error::ByteLimit));
        if let Some(prior) = &prior_text {
            assert!(prior < &text, "hex changed term order");
        }
        prior_text = Some(text);
    }
    assert_eq!(term_bytes(usize::MAX), Err(Error::ByteLimit));
    assert!(matches!(key.encrypt_prefix(b"x", 8), Err(Error::ByteLimit)));
    let good = terms[0].to_storage_text(LIMIT)?;
    let mut uppercase = good.clone();
    uppercase.replace_range(2..3, "A");
    let mut carry = good.clone();
    carry.replace_range(0..2, "02");
    for invalid in [
        String::new(),
        uppercase,
        carry,
        good[..good.len() - 1].to_owned(),
        "00".repeat(10),
    ] {
        assert_eq!(
            RangeTerm::from_storage_text(&invalid, LIMIT),
            Err(Error::InvalidTerm)
        );
    }
    Ok(())
}
