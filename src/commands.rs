use std::path::Path;
use crate::df::Result;

pub fn size(path: &Path) -> Result<()> {
    let df = crate::df::data_frame(path)?;
    println!("{}", df.row_count()?);
    Ok(())
}
