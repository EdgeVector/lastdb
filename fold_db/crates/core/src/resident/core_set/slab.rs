//! Chunked id table for the warm set's slot, path, and source tables.
//!
//! A `Vec<Option<T>>` doubles its capacity as it grows, so up to half of it is
//! unused. Measured 2026-10-20: 9,000 warm slots sat in a 16,384-entry vector,
//! about 1.1 MB of empty entries for 3,000 records. Fixed chunks bound the
//! unused part to one chunk. Freed ids are reused before the table grows.
//!
//! The gauge charges entries, not chunks, and chunks stay after a purge. So
//! each table holds up to one uncharged partial chunk: about 0.5 MB for the
//! three tables together (a slot chunk is about 300 KB with the inline tip).

/// Entries per chunk.
const CHUNK: usize = 1024;

pub(super) struct Slab<T> {
    chunks: Vec<Box<[Option<T>]>>,
    free: Vec<u32>,
    /// One past the highest id ever handed out.
    next: u32,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            free: Vec::new(),
            next: 0,
        }
    }
}

fn split(id: u32) -> (usize, usize) {
    let id = id as usize;
    (id / CHUNK, id % CHUNK)
}

fn new_chunk<T>() -> Result<Box<[Option<T>]>, std::collections::TryReserveError> {
    let mut chunk = Vec::new();
    chunk.try_reserve_exact(CHUNK)?;
    chunk.resize_with(CHUNK, || None);
    Ok(chunk.into_boxed_slice())
}

impl<T> Slab<T> {
    /// Store `value`, reusing a freed id before the table grows.
    pub(super) fn insert(&mut self, value: T) -> u32 {
        let id = self.free.pop().unwrap_or_else(|| {
            let id = self.next;
            self.next = id.checked_add(1).expect("core table id space");
            id
        });
        let (chunk, at) = split(id);
        while self.chunks.len() <= chunk {
            // Out of memory aborts, as `Vec::push` did. A panic here would
            // poison the warm set lock for every later caller.
            let fresh = new_chunk().unwrap_or_else(|_| {
                std::alloc::handle_alloc_error(std::alloc::Layout::new::<[Option<T>; CHUNK]>())
            });
            self.chunks.push(fresh);
        }
        self.chunks[chunk][at] = Some(value);
        id
    }

    pub(super) fn get(&self, id: u32) -> Option<&T> {
        let (chunk, at) = split(id);
        self.chunks.get(chunk)?[at].as_ref()
    }

    pub(super) fn get_mut(&mut self, id: u32) -> Option<&mut T> {
        let (chunk, at) = split(id);
        self.chunks.get_mut(chunk)?[at].as_mut()
    }

    /// Remove the value and free its id.
    pub(super) fn take(&mut self, id: u32) -> Option<T> {
        let (chunk, at) = split(id);
        let value = self.chunks.get_mut(chunk)?[at].take()?;
        self.free.push(id);
        Some(value)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u32, &T)> {
        self.chunks.iter().enumerate().flat_map(|(chunk, entries)| {
            entries
                .iter()
                .enumerate()
                .filter_map(move |(at, entry)| Some(((chunk * CHUNK + at) as u32, entry.as_ref()?)))
        })
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (u32, &mut T)> {
        self.chunks
            .iter_mut()
            .enumerate()
            .flat_map(|(chunk, entries)| {
                entries
                    .iter_mut()
                    .enumerate()
                    .filter_map(move |(at, entry)| {
                        Some(((chunk * CHUNK + at) as u32, entry.as_mut()?))
                    })
            })
    }
}
