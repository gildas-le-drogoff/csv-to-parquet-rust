// ============================================================
use anyhow::Result;
use csv::ReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
pub fn count_lines<P: AsRef<Path>>(path: P, delimiter: u8, has_header: bool) -> Result<usize> {
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(true)
        .from_path(path)?;
    Ok(reader.records().count())
}
pub fn count_parquet_lines<P: AsRef<Path>>(path: P) -> Result<usize> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum())
}
