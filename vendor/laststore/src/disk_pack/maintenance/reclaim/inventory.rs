use super::*;

pub(super) fn generations(
    root: &Path,
    max_files: usize,
    published: &BTreeSet<u64>,
    ceiling: Option<u64>,
) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for (seen, entry) in std::fs::read_dir(root.join("generations"))?.enumerate() {
        let entry = entry?;
        // Each generation owns a receipt file. Use the home inventory budget;
        // a lower independent limit can permanently prevent valid cleanup.
        if seen >= max_files || !entry.file_type()?.is_dir() {
            return Err(corrupt(
                "generation inventory exceeds bound or contains a non-directory",
            ));
        }
        let number = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
            .filter(|number| *number > 0)
            .ok_or_else(|| corrupt("invalid generation directory"))?;
        // Newer receipt directories may still be under construction.
        if ceiling.is_some_and(|floor| number > floor) && !published.contains(&number) {
            continue;
        }
        if !published.contains(&number) && remove_unfinished(&entry.path())? {
            continue;
        }
        out.push(number);
    }
    Ok(out)
}
