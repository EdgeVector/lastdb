//! Merge bounded administrative ranges across the public and private lanes.

use laststore::disk_run::home::{RunHashRange, RunRangePage};
use laststore::Result;

pub(in crate::storage) struct MergedRange<'a> {
    public: Side<'a>,
    private: Side<'a>,
}

struct Side<'a> {
    cursor: RunHashRange<'a>,
    head: Option<(String, Vec<u8>)>,
    exhausted: bool,
    resume: Option<String>,
}

impl<'a> MergedRange<'a> {
    pub(in crate::storage) fn new(public: RunHashRange<'a>, private: RunHashRange<'a>) -> Self {
        Self {
            public: Side::new(public),
            private: Side::new(private),
        }
    }

    pub(in crate::storage) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> Result<()> {
        self.public
            .cursor
            .account_memory_record(key_bytes, body_bytes)
    }

    pub(in crate::storage) fn page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
    ) -> Result<RunRangePage> {
        let mut records = Vec::with_capacity(limit.min(128));
        while records.len() < limit {
            let charge = yielding.map(|(held, per_row)| (held + records.len(), per_row));
            self.public.fill(charge)?;
            self.private.fill(charge)?;
            let resume = self
                .public
                .resume
                .iter()
                .chain(self.private.resume.iter())
                .min();
            let ready = self
                .public
                .head
                .iter()
                .chain(self.private.head.iter())
                .min_by_key(|(key, _)| key);
            if let Some(bound) = resume {
                if ready.is_none_or(|(key, _)| key > bound) {
                    return Ok(RunRangePage {
                        records,
                        exhausted: false,
                        resume: Some(bound.clone()),
                    });
                }
            }
            let Some((key, _)) = ready else {
                break;
            };
            if self
                .public
                .head
                .as_ref()
                .is_some_and(|(found, _)| found == key)
            {
                if self
                    .private
                    .head
                    .as_ref()
                    .is_some_and(|(found, _)| found == key)
                {
                    return Err(laststore::Error::Corrupt(
                        "shared disk key appears in two lanes".into(),
                    ));
                }
                records.push(self.public.head.take().expect("public range head"));
            } else {
                records.push(self.private.head.take().expect("private range head"));
            }
        }
        Ok(RunRangePage {
            records,
            exhausted: self.public.done() && self.private.done(),
            resume: None,
        })
    }
}

impl<'a> Side<'a> {
    fn new(cursor: RunHashRange<'a>) -> Self {
        Self {
            cursor,
            head: None,
            exhausted: false,
            resume: None,
        }
    }

    fn done(&self) -> bool {
        self.exhausted && self.head.is_none()
    }

    fn fill(&mut self, yielding: Option<(usize, usize)>) -> Result<()> {
        if self.exhausted || self.head.is_some() || self.resume.is_some() {
            return Ok(());
        }
        let mut page = match yielding {
            Some((held, per_row)) => self.cursor.next_page_yielding(1, held, per_row)?,
            None => self.cursor.next_page(1)?,
        };
        self.head = page.records.pop();
        self.exhausted = page.exhausted;
        self.resume = page.resume;
        Ok(())
    }
}
