use std::fmt::Display;
use std::io;

pub type Result<A> = std::result::Result<A, ParquetFileError>;

#[derive(Debug)]
pub enum ParquetFileError {
    CorruptedFile(String),
    IOError(io::Error),
}

impl From<io::Error> for ParquetFileError {
    fn from(err: io::Error) -> ParquetFileError {
        ParquetFileError::IOError(err)
    }
}

impl ParquetFileError {
    pub fn corrupted<T: Display>(err: T) -> ParquetFileError {
        ParquetFileError::CorruptedFile(format!("{}", err))
    }
}
