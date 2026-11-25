//! Minimal deterministic RFC 8259 JSON value parser and serializer for schema merging.

use std::collections::BTreeMap;

const MAX_DEPTH: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JsonVal {
    Null,
    Bool(bool),
    Number(String),
    Str(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

pub(crate) fn parse_json(input: &[u8]) -> Option<JsonVal> {
    let text = std::str::from_utf8(input).ok()?;
    let mut parser = Parser {
        bytes: text.as_bytes(),
        pos: 0,
    };
    let val = parser.parse_value(0)?;
    parser.skip_ws();
    (parser.pos == parser.bytes.len()).then_some(val)
}

pub(crate) fn encode_json(val: &JsonVal) -> Vec<u8> {
    let mut out = String::new();
    write_val(&mut out, val);
    out.into_bytes()
}

fn write_val(out: &mut String, val: &JsonVal) {
    match val {
        JsonVal::Null => out.push_str("null"),
        JsonVal::Bool(true) => out.push_str("true"),
        JsonVal::Bool(false) => out.push_str("false"),
        JsonVal::Number(raw) => out.push_str(raw),
        JsonVal::Str(s) => write_str(out, s),
        JsonVal::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_val(out, item);
            }
            out.push(']');
        }
        JsonVal::Object(map) => {
            out.push('{');
            for (idx, (k, v)) in map.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_str(out, k);
                out.push(':');
                write_val(out, v);
            }
            out.push('}');
        }
    }
}

fn write_str(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let b = c as usize;
                out.push_str("\\u00");
                out.push(HEX[(b >> 4) & 0xf] as char);
                out.push(HEX[b & 0xf] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len()
            && matches!(self.bytes[self.pos], b' ' | b'\t' | b'\r' | b'\n')
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn eat(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_kw(&mut self, kw: &[u8]) -> bool {
        if self.bytes.get(self.pos..self.pos + kw.len()) == Some(kw) {
            self.pos += kw.len();
            true
        } else {
            false
        }
    }

    fn parse_value(&mut self, depth: usize) -> Option<JsonVal> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.skip_ws();
        match self.peek()? {
            b'n' => self.eat_kw(b"null").then_some(JsonVal::Null),
            b't' => self.eat_kw(b"true").then_some(JsonVal::Bool(true)),
            b'f' => self.eat_kw(b"false").then_some(JsonVal::Bool(false)),
            b'"' => self.parse_string().map(JsonVal::Str),
            b'[' => self.parse_array(depth + 1),
            b'{' => self.parse_object(depth + 1),
            b'-' | b'0'..=b'9' => self.parse_number().map(JsonVal::Number),
            _ => None,
        }
    }

    fn parse_array(&mut self, depth: usize) -> Option<JsonVal> {
        self.pos += 1;
        self.skip_ws();
        let mut items = Vec::new();
        if self.eat(b']') {
            return Some(JsonVal::Array(items));
        }
        loop {
            items.push(self.parse_value(depth)?);
            self.skip_ws();
            if self.eat(b',') {
                continue;
            }
            return self.eat(b']').then_some(JsonVal::Array(items));
        }
    }

    fn parse_object(&mut self, depth: usize) -> Option<JsonVal> {
        self.pos += 1;
        self.skip_ws();
        let mut map = BTreeMap::new();
        if self.eat(b'}') {
            return Some(JsonVal::Object(map));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            if !self.eat(b':') {
                return None;
            }
            let val = self.parse_value(depth)?;
            map.insert(key, val);
            self.skip_ws();
            if self.eat(b',') {
                continue;
            }
            return self.eat(b'}').then_some(JsonVal::Object(map));
        }
    }

    fn parse_number(&mut self) -> Option<String> {
        let start = self.pos;
        let _ = self.eat(b'-');
        match self.peek()? {
            b'0' => self.pos += 1,
            b'1'..=b'9' => self.eat_digits(),
            _ => return None,
        }
        if self.eat(b'.') && !self.eat_digits_nonempty() {
            return None;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !self.eat_digits_nonempty() {
                return None;
            }
        }
        std::str::from_utf8(&self.bytes[start..self.pos])
            .ok()
            .map(str::to_owned)
    }

    fn eat_digits(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    fn eat_digits_nonempty(&mut self) -> bool {
        let before = self.pos;
        self.eat_digits();
        self.pos > before
    }

    fn parse_string(&mut self) -> Option<String> {
        if !self.eat(b'"') {
            return None;
        }
        let mut out = String::new();
        loop {
            let b = self.peek()?;
            if b == b'"' {
                self.pos += 1;
                return Some(out);
            }
            if b < 0x20 {
                return None;
            }
            if b == b'\\' {
                self.pos += 1;
                out.push(self.parse_escape()?);
                continue;
            }
            let rest = std::str::from_utf8(&self.bytes[self.pos..]).ok()?;
            let ch = rest.chars().next()?;
            self.pos += ch.len_utf8();
            out.push(ch);
        }
    }

    fn parse_escape(&mut self) -> Option<char> {
        let esc = self.peek()?;
        self.pos += 1;
        match esc {
            b'"' => Some('"'),
            b'\\' => Some('\\'),
            b'/' => Some('/'),
            b'b' => Some('\u{08}'),
            b'f' => Some('\u{0c}'),
            b'n' => Some('\n'),
            b'r' => Some('\r'),
            b't' => Some('\t'),
            b'u' => {
                let hi = self.parse_hex4()?;
                if (0xD800..=0xDBFF).contains(&hi) {
                    if !self.eat_kw(b"\\u") {
                        return None;
                    }
                    let lo = self.parse_hex4()?;
                    if !(0xDC00..=0xDFFF).contains(&lo) {
                        return None;
                    }
                    let cp =
                        0x10000 + (((u32::from(hi) - 0xD800) << 10) | (u32::from(lo) - 0xDC00));
                    char::from_u32(cp)
                } else {
                    char::from_u32(u32::from(hi))
                }
            }
            _ => None,
        }
    }

    fn parse_hex4(&mut self) -> Option<u16> {
        let slice = self.bytes.get(self.pos..self.pos + 4)?;
        let s = std::str::from_utf8(slice).ok()?;
        let val = u16::from_str_radix(s, 16).ok()?;
        self.pos += 4;
        Some(val)
    }
}
