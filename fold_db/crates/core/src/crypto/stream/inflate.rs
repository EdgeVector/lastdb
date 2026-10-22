use super::*;
use flate2::{Decompress, FlushDecompress, Status};

/// Require an actual raw-deflate end marker. flate2's reader adapter can return
/// EOF for an incomplete stream; EOF alone is not a compression certificate.
pub(super) fn copy(mut input: impl Read, output: &mut impl Write) -> CryptoResult<()> {
    let mut decoder = Decompress::new(false);
    let mut compressed = zeroize::Zeroizing::new(vec![0; buffer_bytes()]);
    let mut plain = zeroize::Zeroizing::new(vec![0; buffer_bytes()]);
    loop {
        let count = input
            .read(&mut compressed)
            .map_err(|error| io_error(&error))?;
        let mut offset = 0;
        loop {
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder
                .decompress(
                    &compressed[offset..count],
                    &mut plain,
                    if count == 0 {
                        FlushDecompress::Finish
                    } else {
                        FlushDecompress::None
                    },
                )
                .map_err(|_| invalid("encrypted stream deflate format"))?;
            let used = (decoder.total_in() - before_in) as usize;
            let produced = (decoder.total_out() - before_out) as usize;
            output
                .write_all(&plain[..produced])
                .map_err(|error| io_error(&error))?;
            offset += used;
            if status == Status::StreamEnd {
                let mut trailing = [0];
                if offset != count
                    || input
                        .read(&mut trailing)
                        .map_err(|error| io_error(&error))?
                        != 0
                {
                    return Err(invalid("compressed stream has trailing bytes"));
                }
                return Ok(());
            }
            if used == 0 && produced == 0 {
                return Err(invalid(
                    "encrypted stream deflate ended before its final marker",
                ));
            }
            if count != 0 && offset == count {
                break;
            }
        }
    }
}
