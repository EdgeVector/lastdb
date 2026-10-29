//! Prepared identity2 is durable local state, never a ready producer identity.

use super::*;

const MARKER2: &[u8] = b"lastdb-logical-device-capture-v2\n";
const BODY2_BYTES: usize = 145;
const STAGED2: &str = ".logical-device-capture-v2.prepared";

impl Identity {
    pub(in crate::storage::disk_pack::logical_capture) fn has_prepared_at(
        data: &Path,
    ) -> StorageResult<bool> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(data.join(FILE))
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.mode() & 0o777 != 0o600
            || meta.nlink() != 1
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.len() != (MARKER2.len() + BODY2_BYTES) as u64
        {
            return Err(invalid(
                "target private control is not an adoption-prepared identity",
            ));
        }
        let mut marker = [0u8; MARKER2.len()];
        file.read_exact(&mut marker)?;
        let mut state = [0u8; 1];
        file.seek(SeekFrom::End(-1))?;
        file.read_exact(&mut state)?;
        if marker.as_slice() != MARKER2 || state != [2] {
            return Err(invalid("target private identity is not adoption-prepared"));
        }
        Ok(true)
    }

    pub(in crate::storage::disk_pack::logical_capture) fn save_prepared(
        &self,
        data: &Path,
        main: uuid::Uuid,
        cas: uuid::Uuid,
        prepared_root: [u8; 32],
    ) -> StorageResult<()> {
        let bytes = self.prepared_bytes(main, cas, prepared_root);
        let path = data.join(FILE);
        let staged = data.join(STAGED2);
        require_absent(&path)?;
        discard_prepared_stage(&staged)?;
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&staged)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            require_absent(&path)?;
            std::fs::rename(&staged, &path)?;
            File::open(data)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = discard_prepared_stage(&staged);
        }
        result
    }

    pub(in crate::storage::disk_pack::logical_capture) fn verify_prepared(
        &self,
        data: &Path,
        main: uuid::Uuid,
        cas: uuid::Uuid,
        prepared_root: [u8; 32],
    ) -> StorageResult<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(data.join(FILE))?;
        let meta = file.metadata()?;
        let expected = self.prepared_bytes(main, cas, prepared_root);
        if !meta.is_file()
            || meta.mode() & 0o777 != 0o600
            || meta.nlink() != 1
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.len() != expected.len() as u64
        {
            return Err(invalid(
                "prepared snapshot identity is not an exact private file",
            ));
        }
        let mut found = Zeroizing::new(Vec::with_capacity(expected.len()));
        Read::by_ref(&mut file)
            .take(expected.len() as u64 + 1)
            .read_to_end(&mut found)?;
        if found.as_slice() != expected.as_slice() {
            return Err(invalid(
                "prepared snapshot identity differs from the local source",
            ));
        }
        Ok(())
    }

    fn prepared_bytes(
        &self,
        main: uuid::Uuid,
        cas: uuid::Uuid,
        prepared_root: [u8; 32],
    ) -> Zeroizing<Vec<u8>> {
        let mut bytes = Zeroizing::new(Vec::with_capacity(MARKER2.len() + BODY2_BYTES));
        bytes.extend_from_slice(MARKER2);
        bytes.extend_from_slice(self.main.as_bytes());
        bytes.extend_from_slice(self.cas.as_bytes());
        bytes.extend_from_slice(main.as_bytes());
        bytes.extend_from_slice(cas.as_bytes());
        bytes.extend_from_slice(self.seed.as_ref());
        bytes.extend_from_slice(self.generation.as_bytes());
        bytes.extend_from_slice(&prepared_root);
        // State2 is adoption_prepared. The ready0/1 reader never accepts it.
        bytes.push(2);
        bytes
    }
}

fn discard_prepared_stage(path: &Path) -> StorageResult<()> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !meta.is_file()
        || meta.mode() & 0o777 != 0o600
        || meta.nlink() != 1
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.len() > (MARKER2.len() + BODY2_BYTES) as u64
    {
        return Err(invalid(
            "prepared snapshot identity stage ownership differs",
        ));
    }
    std::fs::remove_file(path)?;
    Ok(())
}
