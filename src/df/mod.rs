pub mod parquet;

use std::path::Path;

#[derive(Debug)]
pub enum DataFrameError {
    IOError(String),
    CorruptedFile(String),
    UnsupportedFormat
}

pub type Result<A> = std::result::Result<A, DataFrameError>;

pub struct DataFrame {
    pub rows: u64,
}

impl DataFrame {
    pub fn read(path: &Path) -> Result<DataFrame> {
        if parquet::is_parquet(path)? {
            return parquet::read(path)
        } else {
            Err(DataFrameError::UnsupportedFormat)
        }
    }
}
