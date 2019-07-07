use crate::avrofile::AvroFileError;
use crate::parquetfile::ParquetFileError;
use std::io;

#[derive(Debug)]
pub enum DatasetError {
    IOError(io::Error),
    CorruptedFile(String),
    AvroError(AvroFileError),
    ParquetError(ParquetFileError),
    UnsupportedFormat,
}

pub type Result<A> = std::result::Result<A, DatasetError>;

impl From<io::Error> for DatasetError {
    fn from(err: io::Error) -> Self {
        DatasetError::IOError(err)
    }
}

impl From<ParquetFileError> for DatasetError {
    fn from(err: ParquetFileError) -> Self {
        DatasetError::ParquetError(err)
    }
}

impl From<AvroFileError> for DatasetError {
    fn from(err: AvroFileError) -> Self {
        DatasetError::AvroError(err)
    }
}