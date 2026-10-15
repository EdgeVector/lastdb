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

pub(super) fn write(home: &DiskHome, input: Option<&Path>, command: &str) -> Result<Value> {
    let mut operations = Vec::new();
    let mut length = 256u64;
    if let Some(input) = input {
        let mut input = BufReader::new(File::open(input)?);
        while let Some(row) = next_line(&mut input)? {
            if operations.len() == 4096 {
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
                    (64 + collection.len()
                        + key.len()
                        + body.as_ref().map_or(0, |body| 96 + key.len() + body.len()))
                        as u64,
                )
                .filter(|length| *length <= 128 * 1024 * 1024)
                .ok_or_else(|| invalid("complete batch exceeds 128 MiB"))?;
            operations.push(HomeMutation {
                collection,
                key,
                body,
            });
        }
    }
    if operations.is_empty() && command != "publish" {
        return Err(invalid("append and write require a nonempty complete batch").into());
    }
    let mut writer = home.journal()?;
    let recovered_frontier = writer.current().frontier;
    let mut quote = None;
    let mut held = None;
    let batch = if operations.is_empty() {
        None
    } else {
        let accepted_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()?;
        let reservation = writer.reserve(&operations)?;
        quote = Some(quote_json(reservation.quote()));
        held = Some(pressure_json(writer.pressure()?));
        Some(writer.append_reserved(operations, accepted_ms, reservation)?)
    };
    let after_append = pressure_json(writer.pressure()?);
    if command == "write" || command == "publish" {
        writer.publish()?;
    }
    Ok(
        json!({"recovered_frontier":recovered_frontier,"frontier":writer.current().frontier,
        "generation":writer.current().generation,"durable_batch":batch.as_ref().map(|batch|batch.sequence),
        "operations":batch.as_ref().map_or(0,|batch|batch.locations.len()),
        "journal_quote":quote,"journal_reserved":held,"journal_after_append":after_append,
        "journal_final":pressure_json(writer.pressure()?),
        "published":command!="append"}),
    )
}

pub(super) fn quote_json(quote: laststore::disk_pack::journal::JournalQuote) -> Value {
    json!({"operations":quote.operations(),"logical_bytes":quote.logical_bytes(),
        "physical_reserve":quote.physical_reserve(),"scope":"journal_only"})
}

fn pressure_json(pressure: laststore::disk_pack::journal::JournalPressure) -> Value {
    json!({"reserved_bytes":pressure.reserved_bytes,"durable_suffix_bytes":pressure.durable_suffix_bytes,
        "reserved_batches":pressure.reserved_batches,"oldest_reservation_ms":pressure.oldest_reservation_ms,
        "oldest_unpublished_ms":pressure.oldest_unpublished_ms,"should_materialize":pressure.should_materialize})
}

pub(super) fn plan(home: &DiskHome, path: &Path) -> Result<Value> {
    use laststore::disk_pack::journal::JournalQuote;
    let mut input = BufReader::new(File::open(path)?);
    let mut shapes = Vec::new();
    while let Some(row) = next_line(&mut input)? {
        if shapes.len() == 4096 {
            return Err(invalid("complete batch exceeds 4096 operations").into());
        }
        let body_bytes = if row.get("deleted").and_then(Value::as_bool) == Some(true) {
            None
        } else {
            let value = text(&row, "body_hex")?;
            if value.len() % 2 != 0
                || value.len() > 2 * 1024 * 1024
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(invalid(
                    "body hex requires lowercase complete bytes and at most 1 MiB",
                )
                .into());
            }
            Some((value.len() / 2) as u64)
        };
        shapes.push(home.descriptor().mutation_shape(
            text(&row, "collection")?,
            text(&row, "key")?,
            body_bytes,
        )?);
    }
    Ok(
        json!({"journal_quote":quote_json(JournalQuote::from_shapes(&shapes)?),
        "directory_reserved":false,"logical_records_reserved":false,"memory_ack_allowed":false}),
    )
}
