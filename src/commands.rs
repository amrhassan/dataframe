use crate::ds::*;
use std::path::Path;

pub fn size(path: &Path) -> Result<()> {
    let df = crate::ds::data_frame(path)?;
    println!("{}", df.row_count()?);
    Ok(())
}

pub fn format(path: &Path) -> Result<()> {
    let df = crate::ds::data_frame(path)?;
    println!("{}", df.format());
    Ok(())
}
