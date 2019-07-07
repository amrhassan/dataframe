use crate::avrofile::AvroFileError;
use crate::parquetfile::ParquetFileError;
use std::io;

#[derive(Debug, From)]
pub enum DatasetError {
    IOError(io::Error),
    CorruptedFile(String),
    AvroError(AvroFileError),
    ParquetError(ParquetFileError),
    UnsupportedFormat,
}

pub type Result<A> = std::result::Result<A, DatasetError>;
