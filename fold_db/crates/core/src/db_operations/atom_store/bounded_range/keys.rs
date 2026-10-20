//! Key arithmetic of the range walk: the next key and the middle of a span.

/// The least key above `key`.
pub(super) fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

/// A key strictly between `low` and `high`, near the middle of the key space
/// between them. The keys are read as numbers in base 128 (one ASCII byte per
/// digit). `None` when no ASCII key lies between them.
pub(super) fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|byte| *byte >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let digits = |key: &[u8]| -> Vec<u32> {
        (0..width)
            .map(|at| key.get(at).map_or(0, |byte| u32::from(*byte)))
            .collect()
    };
    let (a, b) = (digits(low), digits(high));
    let mut sum = vec![0u32; width + 1];
    let mut carry = 0;
    for at in (0..width).rev() {
        let total = a[at] + b[at] + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let mut middle = Vec::with_capacity(width);
    let mut remainder = 0;
    for (at, digit) in sum.into_iter().enumerate() {
        let value = remainder * 128 + digit;
        remainder = value % 2;
        if at > 0 {
            middle.push(u8::try_from(value / 2).ok()?);
        }
    }
    while middle.len() > 1 && middle.last() == Some(&0) && middle[..middle.len() - 1] > *low {
        middle.pop();
    }
    (middle.as_slice() > low && middle.as_slice() < high).then_some(middle)
}
