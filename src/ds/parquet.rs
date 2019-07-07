use crate::ds::*;
use std::path::Path;

pub struct ParquetDataset<'a> {
    pub path: &'a Path,
}

impl<'a> Dataset for ParquetDataset<'a> {
    fn format(&self) -> Format {
        Format::Parquet
    }

    fn row_count(&self) -> Result<u64> {
        Ok(parquetfile::row_count(self.path)?)
    }
}
