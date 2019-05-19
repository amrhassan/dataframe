use std::path::Path;
use crate::df::*;

pub fn is_avro(path: &Path) -> Result<bool> {
    // TODO
    Ok(false)
}

pub struct AvroDataFrame<'a> {
    pub path: &'a Path
}

impl <'a> DataFrame for AvroDataFrame<'a> {
    fn row_count(&self) -> Result<u64> {
        // TODO
        Err(DataFrameError::UnsupportedFormat)
    }
}
