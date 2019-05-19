use avro_rs::decode;
use avro_rs::types::Value;
use avro_rs::Schema;
use std::fmt::Display;
use std::fs::File;
use std::io;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

fn io_error<T: Display>(err: T) -> AvroFileError {
    AvroFileError::IOError(format!("{}", err))
}

fn corrupted_file_error<T: Display>(err: T) -> AvroFileError {
    AvroFileError::CorruptedFile(format!("{}", err))
}

fn file_open(path: &Path) -> Result<File> {
    File::open(path).map_err(io_error)
}

fn file_seek(fp: &mut File, pos: SeekFrom) -> Result<u64> {
    fp.seek(pos).map_err(io_error)
}

#[derive(Debug)]
pub enum AvroFileError {
    CorruptedFile(String),
    IOError(String),
}

pub type Result<A> = std::result::Result<A, AvroFileError>;

pub struct AvroFile {
    pub metadata: Value,
    pub blocks: Vec<AvroBlock>,
}

impl AvroFile {
    pub fn read(path: &Path) -> Result<AvroFile> {
        let mut fp = file_open(path)?;

        // Seek past the magic number
        file_seek(&mut fp, SeekFrom::Start(4))?;

        // Seek past the metadata
        let metadata_schema = Schema::Map(Box::new(Schema::Bytes));
        let metadata = decode::decode(&metadata_schema, &mut fp).map_err(corrupted_file_error)?;

        // Seek past the sync marker
        file_seek(&mut fp, SeekFrom::Current(16))?;

        let blocks = AvroBlocks::collect(&mut fp)?;

        Ok(AvroFile { metadata, blocks })
    }
}

/// Returns None when EOF
fn binary_decode_long<R: Read>(r: &mut R) -> Option<Result<i64>> {
    let v = decode::decode(&Schema::Long, r);
    match v {
        Ok(Value::Long(long_value)) => Some(Ok(long_value)),
        Err(err) => match err.downcast_ref::<io::Error>() {
            Some(io_error) if io_error.kind() == ErrorKind::UnexpectedEof => None,
            _ => Some(Err(corrupted_file_error(err))),
        },
        Ok(v) => Some(Err(corrupted_file_error(format!(
            "Unexpected value when decoding a long: {:?}",
            v
        )))),
    }
}

pub struct AvroBlock {
    pub data_offset: u64,
    pub data_length: u64,
    pub object_count: u64,
}

struct AvroBlocks<'a> {
    fp: &'a mut File,
}

impl<'a> Iterator for AvroBlocks<'a> {
    type Item = Result<AvroBlock>;

    fn next(&mut self) -> Option<Result<AvroBlock>> {
        let object_count_res = binary_decode_long(self.fp)?;
        let length_res = binary_decode_long(self.fp)?;
        match (object_count_res, length_res) {
            (Ok(object_count), Ok(length)) => Some(self.next_result(object_count, length)),
            _ => Some(Err(corrupted_file_error("Corrupted block header"))),
        }
    }
}

impl<'a> AvroBlocks<'a> {
    fn next_result(&mut self, object_count: i64, data_length: i64) -> Result<AvroBlock> {
        let offset = file_seek(self.fp, SeekFrom::Current(0))?;
        let block = AvroBlock {
            data_offset: offset,
            data_length: data_length as u64,
            object_count: object_count as u64,
        };
        file_seek(self.fp, SeekFrom::Current(data_length + 16))?;
        Ok(block)
    }

    fn collect(fp: &mut File) -> Result<Vec<AvroBlock>> {
        AvroBlocks { fp }.collect()
    }
}
