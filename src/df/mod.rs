pub mod parquet;

#[derive(Debug)]
pub enum DataFrameError {
    IOError(String),
    CorruptedFile(String)
}

pub type Result<A> = std::result::Result<A, DataFrameError>;
