pub mod avro;
pub mod parquet;

use std::fmt;
use std::path::Path;

pub enum DataFrameError {
    IOError(String),
    CorruptedFile(String),
    UnsupportedFormat,
}

pub type Result<A> = std::result::Result<A, DataFrameError>;

pub enum Format {
    Parquet,
    Avro,
}

pub struct Size {
    pub rows: u64,
    pub columns: u64,
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
    fn size(&self) -> Result<Size>;
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
