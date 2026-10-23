use super::Generation;
use crate::disk_pack::dependency::Coordinate;

/// Both lists are strictly ordered, so immutable prior records need one linear pass.
pub(super) struct Carried<'a> {
    prior: Option<&'a Generation>,
    at: usize,
}

impl<'a> Carried<'a> {
    pub(super) fn new(prior: Option<&'a Generation>) -> Self {
        Self { prior, at: 0 }
    }

    pub(super) fn get(&mut self, coordinate: &Coordinate) -> Option<[u8; 80]> {
        let prior = self.prior?;
        while self.at < prior.coordinates.len() && prior.coordinates[self.at] < *coordinate {
            self.at += 1;
        }
        (prior.coordinates.get(self.at) == Some(coordinate)).then(|| prior.records[self.at])
    }
}
