use crate::ds::*;
use parquet_format::FileMetaData;
use std::fs::File;
use std::io::{SeekFrom, Seek, Read};
use std::path::Path;
use thrift::protocol::TCompactInputProtocol;

fn read_file_metadata_length(fp: &mut File) -> Result<u32> {
    let mut buff = [0; 4];
    fp.seek(SeekFrom::End(-8))?;
    fp.read_exact(&mut buff)?;
    Ok(u32::from_le_bytes(buff))
}

fn read_file_metadata(path: &Path) -> Result<parquet::FileMetaData> {
    let mut fp = &mut File::open(path)?;

    let file_metadata_length = read_file_metadata_length(&mut fp)?;
    fp.seek(SeekFrom::End(-8 - file_metadata_length as i64))?;

    let protocol = &mut TCompactInputProtocol::new(fp);
    FileMetaData::read_from_in_protocol(protocol)
        .map_err(|err| DatasetError::CorruptedFile(format!("{}", err)))
}

pub struct ParquetDataset<'a> {
    pub path: &'a Path,
}

impl<'a> Dataset for ParquetDataset<'a> {
    fn format(&self) -> Format {
        Format::Parquet
    }

    fn row_count(&self) -> Result<u64> {
        let md = read_file_metadata(self.path)?;
        Ok(md.num_rows as u64)
    }
}
