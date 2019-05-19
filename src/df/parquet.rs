use std::path::Path;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::fs::File;
use std::str;
use crate::parquet;
use thrift::protocol::TCompactInputProtocol;
use crate::df::*;
use std::fmt::Display;

fn io_error<T: Display>(err: T) -> DataFrameError {
    DataFrameError::IOError(format!("{}", err))
}

fn file_open(path: &Path) -> Result<File> {
    File::open(path).map_err(io_error)
}

fn file_seek(fp: &mut File, pos: SeekFrom) -> Result<()> {
    fp.seek(pos).map_err(io_error)?;
    Ok(())
}

fn file_read(fp: &mut File, buff: &mut [u8]) -> Result<()> {
    fp.read_exact(buff).map_err(io_error)
}

pub fn is_parquet(path: &Path) -> Result<bool> {
    let fp = &mut file_open(path)?;
    let mut buff = [0; 4];

    file_read(fp, &mut buff)?;
    let header_ok = str::from_utf8(&buff).map(|v| v == "PAR1").unwrap_or(false);

    file_seek(fp, SeekFrom::End(-4))?;
    buff = [0; 4];
    file_read(fp, &mut buff)?;
    let footer_ok = str::from_utf8(&buff).map(|v| v == "PAR1").unwrap_or(false);

    Ok(header_ok && footer_ok)
}

fn read_file_metadata_length(fp: &mut File) -> Result<u32> {
    let mut buff = [0; 4];
    file_seek(fp, SeekFrom::End(-8))?;
    file_read(fp, &mut buff)?;
    Ok(u32::from_le_bytes(buff))
}

fn read_file_metadata(path: &Path) -> Result<parquet::FileMetaData> {
    let mut fp = &mut file_open(path)?;

    let file_metadata_length = read_file_metadata_length(&mut fp)?;
    file_seek(fp, SeekFrom::End(-8 - file_metadata_length as i64))?;

    let protocol = &mut TCompactInputProtocol::new(fp);
    parquet::FileMetaData::read_from_in_protocol(protocol)
        .map_err(|err| DataFrameError::CorruptedFile(format!("{}", err)))
}

pub struct ParquetDataFrame<'a> {
    pub path: &'a Path
}

impl <'a> DataFrame for ParquetDataFrame<'a> {
    fn row_count(&self) -> Result<u64> {
        read_file_metadata(self.path).map(|md| md.num_rows as u64)
    }
}
