use crate::ds::*;
use std::fmt::Display;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

pub fn io_error<T: Display>(err: T) -> DataFrameError {
    DataFrameError::IOError(format!("{}", err))
}

pub fn file_open(path: &Path) -> Result<File> {
    File::open(path).map_err(io_error)
}

pub fn file_seek(fp: &mut File, pos: SeekFrom) -> Result<u64> {
    fp.seek(pos).map_err(io_error)
}

pub fn file_read(fp: &mut File, buff: &mut [u8]) -> Result<()> {
    fp.read_exact(buff).map_err(io_error)
}
