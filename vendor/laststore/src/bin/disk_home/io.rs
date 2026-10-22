use super::*;
use laststore::disk_pack::home::HomeMutation;
use std::path::Path;

pub(super) fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("missing string field").into())
}

pub(super) fn next_line(input: &mut impl BufRead) -> Result<Option<Value>> {
    let mut bytes = Vec::new();
    let count = input.take(LINE_BYTES + 1).read_until(b'\n', &mut bytes)?;
    if count == 0 {
        return Ok(None);
    }
    if count as u64 > LINE_BYTES {
        return Err(invalid("input line exceeds finite limit").into());
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

pub(super) fn request(path: &Path) -> Result<Value> {
    let mut input = File::open(path)?;
    if input.metadata()?.len() > LINE_BYTES {
        return Err(invalid("request exceeds finite limit").into());
    }
    let mut bytes = Vec::new();
    (&mut input).take(LINE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LINE_BYTES {
        return Err(invalid("request exceeds finite limit").into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 || value.len() > 2 * 1024 * 1024 {
        return Err(invalid("body hex requires complete bytes and at most 1 MiB").into());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let nibble = |byte: u8| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err(invalid("hex requires lowercase digits")),
            };
            Ok((nibble(pair[0])? << 4) | nibble(pair[1])?)
        })
        .collect()
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

fn operations(path: &Path) -> Result<Vec<HomeMutation>> {
    let mut result = Vec::new();
    let mut length = 256u64;
    let mut input = BufReader::new(File::open(path)?);
    while let Some(row) = next_line(&mut input)? {
        if result.len() == 4096 {
            return Err(invalid("complete batch exceeds 4096 operations").into());
        }
        let collection = text(&row, "collection")?.to_string();
        let key = text(&row, "key")?.to_string();
        let body = if row.get("deleted").and_then(Value::as_bool) == Some(true) {
            None
        } else {
            Some(decode_hex(text(&row, "body_hex")?)?)
        };
        length = length
            .checked_add(
                (160 + collection.len() + 2 * key.len() + body.as_ref().map_or(0, Vec::len)) as u64,
            )
            .filter(|length| *length <= 128 * 1024 * 1024)
            .ok_or_else(|| invalid("complete batch exceeds 128 MiB"))?;
        result.push(HomeMutation {
            collection,
            key,
            body,
        });
    }
    if result.is_empty() {
        return Err(invalid("write and plan require a nonempty complete batch").into());
    }
    Ok(result)
}

pub(super) fn write(home: &DiskHome, input: &Path) -> Result<Value> {
    let operations = operations(input)?;
    let count = operations.len();
    let mut writer = home.snapshot()?;
    let previous_frontier = writer.current().frontier;
    let accepted_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let reservation = writer.reserve_mutation(&operations)?;
    let quote = quote_json(reservation.quote());
    let publication = writer.commit_mutation(operations, accepted_ms, reservation)?;
    Ok(json!({
        "previous_frontier":previous_frontier,
        "frontier":publication.current.frontier,
        "generation":publication.current.generation,
        "operations":count,"snapshot_quote":quote,"published":true,
    }))
}

fn quote_json(quote: laststore::disk_pack::home::MutationQuote) -> Value {
    json!({
        "physical_bytes":quote.physical_bytes(),"map_pages":quote.map_pages(),
        "dependency_parts":quote.dependency_parts(),"scope":"complete_snapshot",
    })
}

pub(super) fn plan(home: &DiskHome, path: &Path) -> Result<Value> {
    let operations = operations(path)?;
    let quote = home.descriptor().quote_mutation(&operations)?;
    Ok(json!({
        "snapshot_quote":quote_json(quote),"operations":operations.len(),
        "disk_reserved":false,"logical_records_reserved":false,"memory_ack_allowed":false,
    }))
}
