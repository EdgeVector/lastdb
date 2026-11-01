//! Bounded distinct-tip-key estimator carried by the request accumulator.

use std::hash::{Hash, Hasher};

/// A bounded estimate of distinct field-key-tip keys in resolved field results.
/// Only register ranks survive; no field key or record key is retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TipKeySketch {
    registers: [u8; 1024],
}

impl Default for TipKeySketch {
    fn default() -> Self {
        Self {
            registers: [0; 1024],
        }
    }
}

impl TipKeySketch {
    const PRECISION: u32 = 10;

    pub fn insert(&mut self, field_key_id: &str, hash: &str, range: &str) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (field_key_id, hash, range).hash(&mut hasher);
        let fingerprint = hasher.finish();
        let index = (fingerprint >> (64 - Self::PRECISION)) as usize;
        let remainder = fingerprint << Self::PRECISION;
        let rank = (remainder.leading_zeros() + 1).min(65 - Self::PRECISION) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    pub fn merge(&mut self, other: &Self) {
        for (left, right) in self.registers.iter_mut().zip(other.registers.iter()) {
            *left = (*left).max(*right);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|rank| *rank == 0)
    }

    /// HyperLogLog estimate; linear correction keeps small key sets useful.
    pub fn estimate(&self) -> u64 {
        let m = self.registers.len() as f64;
        let zeroes = self.registers.iter().filter(|rank| **rank == 0).count();
        if zeroes == self.registers.len() {
            return 0;
        }
        let sum: f64 = self
            .registers
            .iter()
            .map(|rank| 2f64.powi(-i32::from(*rank)))
            .sum();
        let raw = 0.720540758 * m * m / sum;
        let estimate = if raw <= 2.5 * m && zeroes > 0 {
            m * (m / zeroes as f64).ln()
        } else {
            raw
        };
        estimate.round() as u64
    }
}
