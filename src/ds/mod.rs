pub mod avro;
pub mod parquet;

mod error;

use std::fmt;
use std::path::Path;
use crate::parquetfile;
use crate::avrofile;

pub use error::DatasetError;
pub use error::Result;

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

pub trait Dataset {
    fn format(&self) -> Format;
    fn row_count(&self) -> Result<u64>;
}

pub fn dataset<'a>(path: &'a Path) -> Result<Box<'a + Dataset>> {
    if parquetfile::is_parquet(path)? {
        Ok(Box::new(parquet::ParquetDataset { path }))
    } else if avrofile::is_avro(path)? {
        Ok(Box::new(avro::AvroDataset { path }))
    } else {
        Err(DatasetError::UnsupportedFormat)
    }
}
