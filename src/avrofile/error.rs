use std::io;
use std::fmt::Display;

pub type Result<A> = std::result::Result<A, AvroFileError>;

#[derive(Debug, From)]
pub enum AvroFileError {
    CorruptedFile(String),
    IOError(io::Error),
}

impl AvroFileError {
    pub fn corrupted<T: Display>(err: T) -> AvroFileError {
        AvroFileError::CorruptedFile(format!("{}", err))
    }
}
