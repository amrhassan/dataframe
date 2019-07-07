use crate::ds::*;
use std::path::Path;

pub struct AvroDataset<'a> {
    pub path: &'a Path,
}

impl<'a> Dataset for AvroDataset<'a> {
    fn format(&self) -> Format {
        Format::Avro
    }

    fn row_count(&self) -> Result<u64> {
        Ok(avrofile::row_count(self.path)?)
    }
}
