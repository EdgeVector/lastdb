use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationLogPage {
    pub version: u16,
    pub page_index: u32,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub files: Vec<ClientNativeFile>,
}
impl MutationLogPage {
    pub fn validate_for(&self, begin: &Begin) -> Result<()> {
        context(
            self.version,
            &self.begin_sha256,
            &self.descriptor_sha256,
            begin,
        )?;
        if self.page_index as usize >= MAX_CLIENT_PAGES
            || self.files.is_empty()
            || self.files.len() > MAX_PAGE_FILES
        {
            return Err(LogCodecError("mutation log page resource bound"));
        }
        let mut previous = None;
        for file in &self.files {
            file.validate()
                .map_err(|_| LogCodecError("mutation log file extents"))?;
            if previous.is_some_and(|old| old >= file.path.as_str()) {
                return Err(LogCodecError("mutation log page path order"));
            }
            previous = Some(file.path.as_str());
        }
        encoded_bound(self)
    }
    pub fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        let value: Self = decode(bytes, key)?;
        value.validate_for(begin)?;
        Ok(value)
    }
}
