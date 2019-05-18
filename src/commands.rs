use std::path::Path;
use crate::df::DataFrame;
use crate::df::Result;

pub fn size(path: &Path) -> Result<()> {
    let df = DataFrame::read(path)?;
    println!("{}", df.rows);
    Ok(())
}