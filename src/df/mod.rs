pub mod parquet;
pub mod avro;

use std::path::Path;

pub enum DataFrameError {
    IOError(String),
    CorruptedFile(String),
    UnsupportedFormat
}

pub type Result<A> = std::result::Result<A, DataFrameError>;

pub trait DataFrame {
    fn row_count(&self) -> Result<u64>;
}

pub fn data_frame<'a>(path: &'a Path) -> Result<Box<'a + DataFrame>> {
    if parquet::is_parquet(path)? {
        return Ok(Box::new(parquet::ParquetDataFrame { path }))
    } else if avro::is_avro(path)? {
        return Ok(Box::new(avro::AvroDataFrame { path }))
    } else {
        Err(DataFrameError::UnsupportedFormat)
    }
}
