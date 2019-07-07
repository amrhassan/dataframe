use std::fmt::Display;
use std::io;

pub type Result<A> = std::result::Result<A, ParquetFileError>;

#[derive(Debug, From)]
pub enum ParquetFileError {
    CorruptedFile(String),
    IOError(io::Error),
}

impl ParquetFileError {
    pub fn corrupted<T: Display>(err: T) -> ParquetFileError {
        ParquetFileError::CorruptedFile(format!("{}", err))
    }
}

impl From<thrift::Error> for ParquetFileError {
    fn from(err: thrift::Error) -> Self {
        ParquetFileError::corrupted(err)
    }
}
