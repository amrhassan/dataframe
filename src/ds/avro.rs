use crate::avrofile::FileMetadata;
use crate::ds::*;
use std::path::Path;
use std::fs::File;
use std::io::{Read, Seek};



pub struct AvroDataset<'a> {
    pub path: &'a Path,
}

impl<'a> Dataset for AvroDataset<'a> {
    fn format(&self) -> Format {
        Format::Avro
    }

    fn row_count(&self) -> Result<u64> {
        let blocks = FileMetadata::read(self.path)
            .map_err(DatasetError::AvroError)?
            .blocks;
        Ok(blocks.into_iter().map(|block| block.object_count).sum())
    }
}
