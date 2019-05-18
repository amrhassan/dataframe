use std::path::Path;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::fs::File;
use std::str;
use crate::parquet;
use thrift::protocol::TCompactInputProtocol;
use crate::df::Result;
use crate::df::DataFrameError;

pub fn is_parquet(path: &Path) -> std::io::Result<bool> {
    let mut fp = File::open(path)?;
    let mut buff = [0; 4];

    fp.read_exact(&mut buff);
    let header_ok = str::from_utf8(&buff).map(|v| v == "PAR1").unwrap_or(false);

    fp.seek(SeekFrom::End(-4));
    buff = [0; 4];
    fp.read_exact(&mut buff);
    let footer_ok = str::from_utf8(&buff).map(|v| v == "PAR1").unwrap_or(false);

    Ok(header_ok && footer_ok)
}

fn read_file_metadata_length(fp: &mut File) -> Result<u32> {
    let mut buff = [0; 4];
    fp.seek(SeekFrom::End(-8));
    fp.read_exact(&mut buff);
    Ok(u32::from_le_bytes(buff))
}

pub fn file_metadata(path: &Path) -> Result<parquet::FileMetaData> {
    let mut fp = File::open(path).map_err(|err| DataFrameError::IOError(format!("{}", err)))?;

    let file_metadata_length = read_file_metadata_length(&mut fp)?;
    fp.seek(SeekFrom::End(-8 - file_metadata_length as i64));

    let mut protocol = TCompactInputProtocol::new(&fp);
    parquet::FileMetaData::read_from_in_protocol(&mut protocol).map_err(|err| DataFrameError::CorruptedFile(format!("{}", err)))
}
