pub mod avro;
pub mod parquet;

mod utils;

use crate::avrofile::AvroFileError;
use std::fmt;
use std::path::Path;
use utils::*;

#[derive(Debug)]
pub enum DataFrameError {
    IOError(String),
    CorruptedFile(String),
    AvroError(AvroFileError),
    UnsupportedFormat,
}

pub type Result<A> = std::result::Result<A, DataFrameError>;

pub enum Format {
    Parquet,
    Avro,
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Format::Avro => write!(f, "Avro"),
            Format::Parquet => write!(f, "Parquet"),
        }
    }
}

pub trait DataFrame {
    fn format(&self) -> Format;
    fn row_count(&self) -> Result<u64>;
}

pub fn data_frame<'a>(path: &'a Path) -> Result<Box<'a + DataFrame>> {
    if parquet::is_parquet(path)? {
        Ok(Box::new(parquet::ParquetDataFrame { path }))
    } else if avro::is_avro(path)? {
        Ok(Box::new(avro::AvroDataFrame { path }))
    } else {
        Err(DataFrameError::UnsupportedFormat)
    }
}
