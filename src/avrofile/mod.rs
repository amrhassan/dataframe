mod error;
mod io;

pub use error::AvroFileError;
use error::*;
use io::*;

use avro_rs::decode;
use avro_rs::{types::Value, Schema};
use std::fs::File;
use std::io::{ErrorKind, Read, SeekFrom};
use std::path::Path;

/// The metadata of an Avro Object Container File
/// https://avro.apache.org/docs/1.8.2/spec.html#Object+Container+Files
pub struct FileMetadata {
    pub metadata: Value,
    pub blocks: Vec<AvroBlockMetadata>,
}

impl FileMetadata {
    pub fn read(path: &Path) -> Result<FileMetadata> {
        let mut fp = file_open(path)?;

        // Seek past the magic number
        file_seek(&mut fp, SeekFrom::Start(4))?;

        // Seek past the metadata
        let metadata_schema = Schema::Map(Box::new(Schema::Bytes));
        let metadata =
            decode::decode(&metadata_schema, &mut fp).map_err(AvroFileError::corrupted)?;

        // Seek past the sync marker
        file_seek(&mut fp, SeekFrom::Current(16))?;

        let blocks = AvroBlockMetadataIterator::collect(&mut fp)?;

        Ok(FileMetadata { metadata, blocks })
    }
}

/// Decode a long out of the byte stream if not EOF, otherwise return None
fn binary_decode_long<R: Read>(r: &mut R) -> Option<Result<i64>> {
    let v = decode::decode(&Schema::Long, r);
    match v {
        Ok(Value::Long(long_value)) => Some(Ok(long_value)),
        Err(err) => match err.downcast_ref::<std::io::Error>() {
            Some(io_error) if io_error.kind() == ErrorKind::UnexpectedEof => None,
            _ => Some(Err(AvroFileError::corrupted(err))),
        },
        Ok(v) => Some(Err(AvroFileError::corrupted(format!(
            "Unexpected value when decoding a long: {:?}",
            v
        )))),
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
        let object_count_res = binary_decode_long(self.fp)?;
        let length_res = binary_decode_long(self.fp)?;
        match (object_count_res, length_res) {
            (Ok(object_count), Ok(length)) => Some(self.next_result(object_count, length)),
            _ => Some(Err(AvroFileError::corrupted("Corrupted block header"))),
        }
    }
}

impl<'a> AvroBlockMetadataIterator<'a> {
    fn next_result(&mut self, object_count: i64, data_length: i64) -> Result<AvroBlockMetadata> {
        let offset = file_seek(self.fp, SeekFrom::Current(0))?;
        let block = AvroBlockMetadata {
            data_offset: offset,
            data_length: data_length as u64,
            object_count: object_count as u64,
        };
        file_seek(self.fp, SeekFrom::Current(data_length + 16))?;
        Ok(block)
    }

    fn collect(fp: &mut File) -> Result<Vec<AvroBlockMetadata>> {
        AvroBlockMetadataIterator { fp }.collect()
    }
}
