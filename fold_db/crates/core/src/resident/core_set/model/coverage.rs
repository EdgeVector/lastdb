//! One temporary borrowed lookup checks the retained dirty prefix in linear time.

use super::*;

type Slot<'a> = (Option<&'a str>, &'a str, &'a str, &'a str);

struct Coverage<'a> {
    changed: HashSet<(&'a str, &'a str)>,
    collections: HashSet<&'a str>,
    slots: HashSet<Slot<'a>>,
    definitions: HashSet<(usize, &'a str)>,
}

pub(super) fn covered(
    root: &ModelRoot,
    changes: &[CoreModelChange],
    definitions: &[PreparedCoreMemory],
    sources: &[(String, String)],
) -> bool {
    let Some(coverage) = coverage(changes, definitions, sources) else {
        return false;
    };
    for (molecule, model) in &root.molecules {
        for ((hash, range), value) in &model.slots {
            if value.sources.iter().any(|(collection, key)| {
                coverage
                    .changed
                    .contains(&(collection.as_str(), key.as_str()))
            }) && !coverage.slots.contains(&(
                molecule.scope.as_deref(),
                &molecule.molecule,
                hash.as_str(),
                range.as_str(),
            )) {
                return false;
            }
        }
    }
    root.definitions.iter().all(|(key, definition)| {
        !definition.sources().iter().any(|source| match &source.key {
            Some(key) => coverage
                .changed
                .contains(&(source.collection.as_str(), key.as_str())),
            None => coverage.collections.contains(source.collection.as_str()),
        }) || coverage.definitions.contains(&(key.0, key.1.as_str()))
    })
}

fn coverage<'a>(
    changes: &'a [CoreModelChange],
    definitions: &'a [PreparedCoreMemory],
    sources: &'a [(String, String)],
) -> Option<Coverage<'a>> {
    let mut coverage = Coverage {
        changed: HashSet::new(),
        collections: HashSet::new(),
        slots: HashSet::new(),
        definitions: HashSet::new(),
    };
    coverage.changed.try_reserve(sources.len()).ok()?;
    coverage.collections.try_reserve(sources.len()).ok()?;
    coverage.slots.try_reserve(changes.len()).ok()?;
    coverage.definitions.try_reserve(definitions.len()).ok()?;
    for (collection, key) in sources {
        coverage.changed.insert((collection.as_str(), key.as_str()));
        coverage.collections.insert(collection.as_str());
    }
    for change in changes {
        let path = &change.path;
        coverage.slots.insert((
            path.scope.as_deref(),
            path.molecule.as_str(),
            path.hash.as_str(),
            path.range.as_str(),
        ));
    }
    coverage.definitions.extend(
        definitions
            .iter()
            .map(|definition| (definition.key.0, definition.key.1.as_str())),
    );
    Some(coverage)
}
