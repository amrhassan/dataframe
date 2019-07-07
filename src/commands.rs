use crate::ds::*;
use std::path::Path;

pub fn size(path: &Path) -> Result<()> {
    let ds = crate::ds::dataset(path)?;
    println!("{}", ds.row_count()?);
    Ok(())
}

pub fn format(path: &Path) -> Result<()> {
    let ds = crate::ds::dataset(path)?;
    println!("{}", ds.format());
    Ok(())
}
