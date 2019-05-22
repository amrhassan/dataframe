use std::io;
use std::fmt::Display;

pub type Result<A> = std::result::Result<A, AvroFileError>;

#[derive(Debug)]
pub enum AvroFileError {
    CorruptedFile(String),
    IOError(io::Error),
}

impl From<io::Error> for AvroFileError {
    fn from(err: io::Error) -> AvroFileError {
        AvroFileError::IOError(err)
    }
}

impl AvroFileError {
    pub fn corrupted<T: Display>(err: T) -> AvroFileError {
        AvroFileError::CorruptedFile(format!("{}", err))
    }
}
