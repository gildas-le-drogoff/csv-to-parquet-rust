// tests/integration_pipeline_tests.rs
use csv_to_parquet::conversion::convert_csv_to_parquet;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_full_pipeline_row_coherence() {
    let mut csv = NamedTempFile::new().unwrap();
    writeln!(csv, "a,b").unwrap();
    for i in 0..1000 {
        writeln!(csv, "{},{}", i, i * 2).unwrap();
    }
    let output = NamedTempFile::new().unwrap();
    convert_csv_to_parquet(csv.path(), output.path(), true, false).unwrap();
    let file = File::open(output.path()).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_rows: usize = reader
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();
    assert_eq!(parquet_rows, 1000);
}
