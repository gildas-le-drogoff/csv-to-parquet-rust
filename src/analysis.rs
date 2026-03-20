// ============================================================
use crate::utils::*;
use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use csv::ReaderBuilder;
use std::io::Cursor;
use std::sync::atomic::Ordering;
use std::sync::Arc;
mod builders;
mod conversions;
pub mod types;
use crate::analysis::builders::{
    build_binary_column, build_bool_column, build_date32_column, build_default_column,
    build_float64_column, build_int64_column, build_large_binary_column, build_large_utf8_column,
    build_timestamp_column, build_uint64_column, build_utf8_column,
};
use conversions::*;
pub use types::{BlockResult, ColumnMetrics, ConversionResult, ErrorCounters, ErrorSample};
pub fn analyze_block(
    bloc_lignes: &[String],
    schema: Arc<Schema>,
    delimiter: u8,
    force_utf8: bool,
    counters: &ErrorCounters,
) -> Result<BlockResult> {
    let raw_line_count = bloc_lignes.len();
    let content = bloc_lignes.join("\n");
    let mut csv_reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .from_reader(Cursor::new(content));
    let column_count = schema.fields().len();
    let mut produced_record_count = 0usize;
    let mut values: Vec<Vec<String>> = vec![Vec::new(); column_count];
    let mut metrics: Vec<ColumnMetrics> = schema
        .fields()
        .iter()
        .map(|f| ColumnMetrics::new(f.name()))
        .collect();
    for (index_ligne, resultat) in csv_reader.records().enumerate() {
        let record = match resultat {
            Ok(r) => {
                produced_record_count += 1;
                r
            }
            Err(e) => {
                counters.parse_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "{}",
                    error(format!(
                        "[ERREUR CSV] ligne_bloc={} erreur_parse={}",
                        index_ligne, e
                    ))
                );
                continue;
            }
        };
        for i in 0..column_count {
            let v = record.get(i).unwrap_or("").to_string();
            metrics[i].total_values += 1;
            values[i].push(v);
        }
    }
    if produced_record_count != raw_line_count {
        let delta = raw_line_count.saturating_sub(produced_record_count);
        counters
            .erreurs_structure_csv
            .fetch_add(delta, Ordering::Relaxed);
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_count);
    for (i, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Int64 => build_int64_column(&values[i], &mut metrics[i], convert_to_i64),
            DataType::UInt64 => build_uint64_column(&values[i], &mut metrics[i], convert_to_u64),
            DataType::Boolean => build_bool_column(&values[i], &mut metrics[i], convert_to_bool),
            DataType::Float64 => build_float64_column(&values[i], &mut metrics[i], convert_to_f64),
            DataType::Date32 => build_date32_column(&values[i], &mut metrics[i], convert_to_date32),
            DataType::Timestamp(unit, _) => {
                build_timestamp_column(&values[i], &mut metrics[i], convert_to_timestamp_ms, unit)
            }
            DataType::Binary => build_binary_column(&values[i], &mut metrics[i]),
            DataType::LargeBinary => build_large_binary_column(&values[i], &mut metrics[i]),
            DataType::Utf8 => build_utf8_column(&values[i], &mut metrics[i]),
            DataType::LargeUtf8 => build_large_utf8_column(&values[i], &mut metrics[i], force_utf8),
            _ => build_default_column(&values[i], &mut metrics[i]),
        };
        arrays.push(array);
    }
    Ok(BlockResult {
        batch: RecordBatch::try_new(schema, arrays)?,
        metrics,
    })
}
// ============================================================
