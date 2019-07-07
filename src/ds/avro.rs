use crate::avrofile::FileMetadata;
use crate::ds::*;
use std::path::Path;
use utils::*;

pub fn is_avro(path: &Path) -> Result<bool> {
    let mut fp = file_open(path)?;
    let mut buff = [0; 4];
    file_read(&mut fp, &mut buff)?;

    Ok(buff == [0x4F, 0x62, 0x6A, 0x01])
}

pub struct AvroDataFrame<'a> {
    pub path: &'a Path,
}

impl<'a> DataFrame for AvroDataFrame<'a> {
    fn format(&self) -> Format {
        Format::Avro
    }

    fn row_count(&self) -> Result<u64> {
        let blocks = FileMetadata::read(self.path)
            .map_err(DataFrameError::AvroError)?
            .blocks;
        Ok(blocks.into_iter().map(|block| block.object_count).sum())
    }
}
