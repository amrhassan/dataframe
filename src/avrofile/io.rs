use super::error::*;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;

pub fn file_open(path: &Path) -> Result<File> {
    File::open(path).map_err(AvroFileError::from)
}

pub fn file_seek(fp: &mut File, pos: SeekFrom) -> Result<u64> {
    fp.seek(pos).map_err(AvroFileError::from)
}
