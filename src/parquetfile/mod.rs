mod error;

pub use error::ParquetFileError;
use error::*;

use std::path::Path;
use std::io::SeekFrom;
use std::fs::File;
use std::io::{Read, Seek};
use std::str;
use parquet_format::FileMetaData;
use thrift::protocol::TCompactInputProtocol;

pub fn is_parquet(path: &Path) -> Result<bool> {
    let fp = &mut File::open(path)?;
    let mut buff = [0; 4];

    fp.read_exact(&mut buff)?;
    let header_ok = str::from_utf8(&buff).map(|v| v == "PAR1").unwrap_or(false);

    fp.seek(SeekFrom::End(-4))?;
    buff = [0; 4];
    fp.read_exact(&mut buff)?;
    let footer_ok = str::from_utf8(&buff).map(|v| v == "PAR1").unwrap_or(false);

    Ok(header_ok && footer_ok)
}

pub fn row_count(path: &Path) -> Result<u64> {
    let md = read_file_metadata(path)?;
    Ok(md.num_rows as u64)
}

fn read_file_metadata_length(fp: &mut File) -> Result<u32> {
    let mut buff = [0; 4];
    fp.seek(SeekFrom::End(-8))?;
    fp.read_exact(&mut buff)?;
    Ok(u32::from_le_bytes(buff))
}

fn read_file_metadata(path: &Path) -> Result<FileMetaData> {
    let mut fp = &mut File::open(path)?;

    let file_metadata_length = read_file_metadata_length(&mut fp)?;
    fp.seek(SeekFrom::End(-8 - file_metadata_length as i64))?;

    let protocol = &mut TCompactInputProtocol::new(fp);
    Ok(FileMetaData::read_from_in_protocol(protocol)?)
}
