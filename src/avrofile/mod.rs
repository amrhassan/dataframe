mod error;
mod io;

pub use error::AvroFileError;
use error::*;
use io::*;

use avro_rs::{types::Value, Schema};
use std::fs::File;
use std::io::{SeekFrom, Read, Seek};
use std::path::Path;

/// The metadata of an Avro Object Container File
/// https://avro.apache.org/docs/1.8.2/spec.html#Object+Container+Files
pub struct FileMetadata {
    pub metadata: Value,
    pub blocks: Vec<AvroBlockMetadata>,
}

impl FileMetadata {
    pub fn read(path: &Path) -> Result<FileMetadata> {
        let mut fp = File::open(path)?;

        // Seek past the magic number
        fp.seek(SeekFrom::Start(4))?;

        // Seek past the metadata
        let metadata_schema = Schema::Map(Box::new(Schema::Bytes));
        let metadata = decode(&metadata_schema, &mut fp)
            .unwrap_or(Err(AvroFileError::corrupted("Failed to decode file metadata")))?;

        // Seek past the sync marker
        fp.seek(SeekFrom::Current(16))?;

        let blocks = AvroBlockMetadataIterator::collect(&mut fp)?;

        Ok(FileMetadata { metadata, blocks })
    }
}

pub struct AvroBlockMetadata {
    pub data_offset: u64,
    pub data_length: u64,
    pub object_count: u64,
}

struct AvroBlockMetadataIterator<'a> {
    fp: &'a mut File,
}

impl<'a> Iterator for AvroBlockMetadataIterator<'a> {
    type Item = Result<AvroBlockMetadata>;

    fn next(&mut self) -> Option<Result<AvroBlockMetadata>> {
        let object_count_res = decode_long(self.fp)?;
        let length_res = decode_long(self.fp)?;
        match (object_count_res, length_res) {
            (Ok(object_count), Ok(length)) => Some(self.next_result(object_count, length)),
            _ => Some(Err(AvroFileError::corrupted("Corrupted block header"))),
        }
    }
}

impl<'a> AvroBlockMetadataIterator<'a> {
    fn next_result(&mut self, object_count: i64, data_length: i64) -> Result<AvroBlockMetadata> {
        let offset = self.fp.seek(SeekFrom::Current(0))?;
        let block = AvroBlockMetadata {
            data_offset: offset,
            data_length: data_length as u64,
            object_count: object_count as u64,
        };
        self.fp.seek(SeekFrom::Current(data_length + 16))?;
        Ok(block)
    }

    fn collect(fp: &mut File) -> Result<Vec<AvroBlockMetadata>> {
        AvroBlockMetadataIterator { fp }.collect()
    }
}

pub fn is_avro(path: &Path) -> Result<bool> {
    let mut fp = File::open(path)?;
    let mut buff = [0; 4];
    fp.read_exact(&mut buff)?;

    Ok(buff == [0x4F, 0x62, 0x6A, 0x01])
}

pub fn row_count(path: &Path) -> Result<u64> {
    let blocks = FileMetadata::read(path)?.blocks;
    Ok(blocks.into_iter().map(|block| block.object_count).sum())
}
