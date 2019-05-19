use crate::df::*;
use std::path::Path;

pub fn size(path: &Path) -> Result<()> {
    let df = crate::df::data_frame(path)?;
    let size = df.size()?;
    println!("{}x{}", size.rows, size.columns);
    Ok(())
}

pub fn format(path: &Path) -> Result<()> {
    let df = crate::df::data_frame(path)?;
    println!("{}", df.format());
    Ok(())
}
