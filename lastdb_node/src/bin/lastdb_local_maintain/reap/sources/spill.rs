//! Exact-key output; only digests stay in the planner matcher.

use super::super::rules::{key_hash, RuleSet};
use super::super::ReapError;
use fold_db::hex::hex_lower;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(super) struct Spill {
    relative: &'static str,
    path: PathBuf,
    writer: BufWriter<File>,
    hashes: Vec<u128>,
}

impl Spill {
    pub(super) fn new(plan_dir: &Path, relative: &'static str) -> Result<Self, ReapError> {
        let path = plan_dir.join(relative);
        std::fs::create_dir_all(path.parent().expect("exact file has parent"))?;
        Ok(Self {
            relative,
            writer: BufWriter::new(File::create(&path)?),
            path,
            hashes: Vec::new(),
        })
    }

    pub(super) fn add(&mut self, key: &str) -> Result<(), ReapError> {
        writeln!(self.writer, "{}", hex_lower(key.as_bytes()))?;
        self.hashes.push(key_hash(key.as_bytes()));
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<RuleSet, ReapError> {
        self.writer.flush()?;
        drop(self.writer);
        let mut rules = RuleSet::default();
        if self.hashes.is_empty() {
            std::fs::remove_file(self.path)?;
        } else {
            rules.add_exact_file(self.relative, self.hashes);
        }
        rules.finish();
        Ok(rules)
    }
}
