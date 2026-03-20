// tests/conversions_tests.rs
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use csv_to_parquet::analysis::{analyze_block, ErrorCounters};

#[test]
fn test_i64_overflow_via_analysis() {
    let lines = vec!["9223372036854775808".to_string()];
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let counters = ErrorCounters::default();
    let result = analyze_block(&lines, schema, b',', false, &counters).unwrap();
    let m = &result.metrics[0];
    assert_eq!(m.total_conversion_errors, 1);
    assert_eq!(m.total_valid_values, 0);
}

#[test]
fn test_u64_negative_via_analysis() {
    let lines = vec!["-1".to_string()];
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, true)]));
    let counters = ErrorCounters::default();
    let result = analyze_block(&lines, schema, b',', false, &counters).unwrap();
    let m = &result.metrics[0];
    assert_eq!(m.total_conversion_errors, 1);
}
