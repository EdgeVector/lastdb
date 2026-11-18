//! One pass over a `/v1/read` or `/v1/write` body. No heap allocation.
//! A syntax error returns `None`; the caller then counts one item and no schema.

use super::{SCHEMAS_PER_REQUEST, SCHEMA_LIMIT};

pub(super) struct Scan {
    pub(super) items: u64,
    pub(super) extra: u64,
    slots: [Hit; SCHEMAS_PER_REQUEST],
    n: usize,
}

struct Hit {
    items: u64,
    len: u8,
    buf: [u8; SCHEMA_LIMIT],
}

impl Hit {
    const EMPTY: Self = Self {
        items: 0,
        len: 0,
        buf: [0; SCHEMA_LIMIT],
    };
}

impl Scan {
    fn empty() -> Self {
        Self {
            items: 0,
            extra: 0,
            slots: [Hit::EMPTY; SCHEMAS_PER_REQUEST],
            n: 0,
        }
    }

    pub(super) fn visit(&self, mut each: impl FnMut(&str, u64)) {
        for hit in &self.slots[..self.n] {
            let end = usize::from(hit.len);
            let Ok(name) = std::str::from_utf8(&hit.buf[..end]) else {
                continue;
            };
            if !name.is_empty() {
                each(name, hit.items);
            }
        }
    }

    fn note(&mut self, name: &[u8]) {
        if name.is_empty() || name.len() > SCHEMA_LIMIT {
            return;
        }
        for index in 0..self.n {
            let same = {
                let hit = &self.slots[index];
                usize::from(hit.len) == name.len() && &hit.buf[..name.len()] == name
            };
            if same {
                self.slots[index].items = self.slots[index].items.saturating_add(1);
                return;
            }
        }
        if self.n >= SCHEMAS_PER_REQUEST {
            self.extra = self.extra.saturating_add(1);
            return;
        }
        let hit = &mut self.slots[self.n];
        hit.buf[..name.len()].copy_from_slice(name);
        hit.len = u8::try_from(name.len()).unwrap_or(0);
        hit.items = 1;
        self.n += 1;
    }
}

pub(super) fn items(body: &[u8], list_key: &[u8]) -> Option<Scan> {
    if body.is_empty() {
        return None;
    }
    let mut index = skip_ws(body, 0)?;
    if body.get(index) != Some(&b'{') {
        return None;
    }
    index += 1;
    let mut found = None;
    loop {
        index = skip_ws(body, index)?;
        if body.get(index) == Some(&b'}') {
            return found;
        }
        let (next, matches) = key_is(body, index, list_key)?;
        index = skip_ws(body, next)?;
        if body.get(index) != Some(&b':') {
            return None;
        }
        index += 1;
        if matches && found.is_none() {
            let mut scan = Scan::empty();
            index = scan_list(body, index, &mut scan)?;
            found = Some(scan);
        } else {
            index = skip_value(body, index)?;
        }
        index = after_comma(body, index)?;
        if body.get(index) == Some(&b'}') {
            return found;
        }
    }
}

fn scan_list(input: &[u8], mut index: usize, scan: &mut Scan) -> Option<usize> {
    index = skip_ws(input, index)?;
    if input.get(index) != Some(&b'[') {
        return None;
    }
    index += 1;
    loop {
        index = skip_ws(input, index)?;
        if input.get(index) == Some(&b']') {
            return Some(index + 1);
        }
        if input.get(index) == Some(&b'{') {
            index = scan_object(input, index, scan)?;
        } else {
            index = skip_value(input, index)?;
        }
        scan.items = scan.items.saturating_add(1);
        index = after_comma(input, index)?;
        if input.get(index) == Some(&b']') {
            return Some(index + 1);
        }
    }
}

fn scan_object(input: &[u8], mut index: usize, scan: &mut Scan) -> Option<usize> {
    if input.get(index) != Some(&b'{') {
        return None;
    }
    index += 1;
    let mut captured = [0u8; SCHEMA_LIMIT];
    let mut captured_len = 0usize;
    let mut saw = false;
    let mut too_long = false;
    index = skip_ws(input, index)?;
    if input.get(index) == Some(&b'}') {
        return Some(index + 1);
    }
    loop {
        let (next, is_schema) = key_is(input, index, b"schema")?;
        index = skip_ws(input, next)?;
        if input.get(index) != Some(&b':') {
            return None;
        }
        index += 1;
        if is_schema && !saw {
            let start = skip_ws(input, index)?;
            let (next, len, overflow) = take_string(input, start, Some(&mut captured))?;
            index = next;
            captured_len = len;
            too_long = overflow;
            saw = true;
        } else {
            index = skip_value(input, index)?;
        }
        index = after_comma(input, index)?;
        if input.get(index) == Some(&b'}') {
            if saw && !too_long {
                scan.note(&captured[..captured_len]);
            }
            return Some(index + 1);
        }
    }
}

/// The index of the next value, or the closing byte when the container ends.
fn after_comma(input: &[u8], mut index: usize) -> Option<usize> {
    index = skip_ws(input, index)?;
    if input.get(index) != Some(&b',') {
        return Some(index);
    }
    index = skip_ws(input, index + 1)?;
    if matches!(input.get(index), Some(b'}' | b']')) {
        return None;
    }
    Some(index)
}

fn skip_ws(input: &[u8], mut index: usize) -> Option<usize> {
    while input
        .get(index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
    {
        index += 1;
    }
    Some(index)
}

fn skip_value(input: &[u8], mut index: usize) -> Option<usize> {
    index = skip_ws(input, index)?;
    match *input.get(index)? {
        b'{' | b'[' => skip_container(input, index),
        b'"' => take_string(input, index, None).map(|(next, _, _)| next),
        b't' => eat(input, index, b"true"),
        b'f' => eat(input, index, b"false"),
        b'n' => eat(input, index, b"null"),
        b'-' | b'0'..=b'9' => skip_number(input, index),
        _ => None,
    }
}

fn skip_container(input: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    let mut depth = 0u32;
    let mut in_string = false;
    let mut escape = false;
    while index < input.len() {
        let byte = input[index];
        index += 1;
        if in_string {
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => depth = depth.saturating_add(1),
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn eat(input: &[u8], index: usize, literal: &[u8]) -> Option<usize> {
    let end = index + literal.len();
    (input.get(index..end) == Some(literal)).then_some(end)
}

fn skip_number(input: &[u8], mut index: usize) -> Option<usize> {
    if input.get(index) == Some(&b'-') {
        index += 1;
    }
    let start = index;
    if input.get(index) == Some(&b'0') {
        index += 1;
    } else if input.get(index).is_some_and(|byte| byte.is_ascii_digit()) {
        while input.get(index).is_some_and(|byte| byte.is_ascii_digit()) {
            index += 1;
        }
    } else {
        return None;
    }
    if input.get(index) == Some(&b'.') {
        index += 1;
        let fraction = index;
        while input.get(index).is_some_and(|byte| byte.is_ascii_digit()) {
            index += 1;
        }
        if index == fraction {
            return None;
        }
    }
    if matches!(input.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(input.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent = index;
        while input.get(index).is_some_and(|byte| byte.is_ascii_digit()) {
            index += 1;
        }
        if index == exponent {
            return None;
        }
    }
    (index > start).then_some(index)
}

fn key_is(input: &[u8], index: usize, want: &[u8]) -> Option<(usize, bool)> {
    let mut buf = [0u8; 16];
    let (next, len, overflow) = take_string(input, index, Some(&mut buf))?;
    let matched = !overflow && buf[..len] == want[..];
    Some((next, matched))
}

fn take_string(
    input: &[u8],
    mut index: usize,
    mut out: Option<&mut [u8]>,
) -> Option<(usize, usize, bool)> {
    if input.get(index) != Some(&b'"') {
        return None;
    }
    index += 1;
    let (mut written, mut overflow) = (0usize, false);
    loop {
        let byte = *input.get(index)?;
        index += 1;
        if byte == b'"' {
            return Some((index, written, overflow));
        }
        if byte == b'\\' {
            let escaped = *input.get(index)?;
            index += 1;
            if escaped == b'u' {
                let (next, code) = hex4(input, index)?;
                index = next;
                let mut encoded = [0u8; 4];
                let text = char::from_u32(code)?.encode_utf8(&mut encoded);
                write_out(&mut out, &mut written, &mut overflow, text.as_bytes());
            } else {
                write_out(
                    &mut out,
                    &mut written,
                    &mut overflow,
                    &[plain_escape(escaped)?],
                );
            }
        } else if byte < 0x20 {
            return None;
        } else {
            write_out(&mut out, &mut written, &mut overflow, &[byte]);
        }
    }
}

fn plain_escape(byte: u8) -> Option<u8> {
    Some(match byte {
        b'"' | b'\\' | b'/' => byte,
        b'b' => 0x08,
        b'f' => 0x0c,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        _ => return None,
    })
}

fn hex4(input: &[u8], index: usize) -> Option<(usize, u32)> {
    let digits = input.get(index..index + 4)?;
    let mut code = 0u32;
    for byte in digits {
        code = (code << 4) | u32::from(hex_value(*byte)?);
    }
    Some((index + 4, code))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn write_out(out: &mut Option<&mut [u8]>, written: &mut usize, overflow: &mut bool, bytes: &[u8]) {
    if *overflow {
        return;
    }
    let Some(buf) = out.as_deref_mut() else {
        return;
    };
    let end = written.saturating_add(bytes.len());
    if end > buf.len() {
        *overflow = true;
        return;
    }
    buf[*written..end].copy_from_slice(bytes);
    *written = end;
}
