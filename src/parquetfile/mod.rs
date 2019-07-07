mod error;

pub use error::ParquetFileError;
use error::*;

use std::path::Path;
use std::io::SeekFrom;
use std::fs::File;
use std::io::{Read, Seek};
use std::str;

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
