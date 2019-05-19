use crate::df::*;
use std::path::Path;

pub fn is_avro(path: &Path) -> Result<bool> {
    // TODO
    Ok(false)
}

pub struct AvroDataFrame<'a> {
    pub path: &'a Path,
}

impl<'a> DataFrame for AvroDataFrame<'a> {
    fn format(&self) -> Format {
        Format::Avro
    }
    fn row_count(&self) -> Result<u64> {
        // TODO
        Err(DataFrameError::UnsupportedFormat)
    }
}
