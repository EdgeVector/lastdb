//! Compare an already-written key without retaining its complete bytes.
use super::super::bytes::*;
use super::super::{corrupt, key_layout, HEADER_BYTES, PAGES_PER_PART, PAGE_BYTES, PAGE_PAYLOAD};
use super::PageWriter;
use crate::Result;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

impl PageWriter {
    pub(in crate::disk_pack) fn compare_key(&self, entry: &[u8], key: &[u8]) -> Result<Ordering> {
        let length = u32_at(entry, 0) as usize;
        let inline = key_layout::inline(entry);
        let common = inline.len().min(key.len());
        let order = inline[..common].cmp(&key[..common]);
        if order != Ordering::Equal {
            return Ok(order);
        }
        if key.len() <= inline.len() || inline.len() == length {
            return Ok(length.cmp(&key.len()));
        }
        let mut offset = inline.len();
        let suffix = length - offset;
        let mut number = key_layout::overflow(entry);
        let mut buffer = vec![0; PAGE_BYTES];
        let mut part = None;
        while offset < length {
            self.read_key_page(number, &mut part, &mut buffer)?;
            let used = u32_at(&buffer, 48) as usize;
            let next = u64_at(&buffer, 64);
            if buffer[10] != 4
                || buffer[11] != 0
                || buffer[14] != 0
                || u16_at(&buffer, 52) != 0
                || u16_at(&buffer, 54) != 0
                || u64_at(&buffer, 56) != 0
                || u64_at(&buffer, 72) != suffix as u64
                || used == 0
                || used > PAGE_PAYLOAD
                || used > length - offset
                || (next != 0 && next >= number)
                || (next == 0) != (used == length - offset)
            {
                return Err(corrupt("private key overflow shape"));
            }
            zero(&buffer[112..128])?;
            zero(&buffer[HEADER_BYTES + used..])?;
            let common = used.min(key.len() - offset);
            let order =
                buffer[HEADER_BYTES..HEADER_BYTES + common].cmp(&key[offset..offset + common]);
            if order != Ordering::Equal {
                return Ok(order);
            }
            offset += common;
            if common < used || offset == key.len() {
                return Ok(length.cmp(&key.len()));
            }
            number = next;
        }
        Ok(length.cmp(&key.len()))
    }

    fn read_key_page(
        &self,
        number: u64,
        part: &mut Option<(u64, File)>,
        buffer: &mut [u8],
    ) -> Result<()> {
        if number == 0 || number >= self.next {
            return Err(corrupt("private key overflow address"));
        }
        let wanted = number / PAGES_PER_PART;
        if part.as_ref().is_none_or(|(index, _)| *index != wanted) {
            let path = self.directory.join(format!("part-{wanted:08x}"));
            let file = File::open(path)?;
            let pages = (self.next - wanted * PAGES_PER_PART).min(PAGES_PER_PART);
            let metadata = file.metadata()?;
            if !metadata.is_file() || metadata.len() != pages * PAGE_BYTES as u64 {
                return Err(corrupt("private key overflow part length"));
            }
            *part = Some((wanted, file));
        }
        let file = &mut part.as_mut().expect("private key part").1;
        file.seek(SeekFrom::Start(number % PAGES_PER_PART * PAGE_BYTES as u64))?;
        file.read_exact(buffer)?;
        if &buffer[..8] != b"LDPAGE1\0"
            || u16_at(buffer, 8) != 1
            || u16_at(buffer, 12) != HEADER_BYTES as u16
            || buffer[15] != 0
            || u64_at(buffer, 16) != self.generation
            || uuid_at(buffer, 24) != self.object
            || u64_at(buffer, 40) != number
        {
            return Err(corrupt("private key overflow identity"));
        }
        verify_checksum(buffer, 80)
    }
}
