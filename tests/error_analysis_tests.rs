// tests/error_analysis_tests.rs
use arrow::datatypes::{DataType, Field, Schema};
use csv_to_parquet::analysis::{analyze_block, ErrorCounters};
use std::sync::Arc;

#[test]
fn test_explicit_null() {
    let lines = vec!["NULL".to_string()];
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let counters = ErrorCounters::default();
    let result = analyze_block(&lines, schema, b',', false, &counters).unwrap();
    let m = &result.metrics[0];
    assert_eq!(m.total_null_text, 1);
    assert_eq!(m.total_valid_values, 0);
}
