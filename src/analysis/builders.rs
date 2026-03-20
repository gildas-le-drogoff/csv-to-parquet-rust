// ============================================================
use super::types::{ColumnMetrics, ConversionResult};
use crate::utils::is_null_text;
use arrow::array::*;
use arrow::datatypes::TimeUnit;
use std::sync::Arc;
pub fn build_int64_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    convertir: fn(&str) -> ConversionResult<i64>,
) -> ArrayRef {
    let mut b = Int64Builder::new();
    for v in values {
        match convertir(v) {
            ConversionResult::Valide(x) => {
                b.append_value(x);
                metrics.total_valid_values += 1;
            }
            ConversionResult::NullExplicite => {
                b.append_null();
                metrics.total_null_text += 1;
            }
            ConversionResult::ErreurConversion(raw) => {
                b.append_null();
                metrics.total_conversion_errors += 1;
                metrics.echantillon.add(raw);
            }
        }
    }
    Arc::new(b.finish())
}
pub fn build_uint64_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    convertir: fn(&str) -> ConversionResult<u64>,
) -> ArrayRef {
    let mut b = UInt64Builder::new();
    for v in values {
        match convertir(v) {
            ConversionResult::Valide(x) => {
                b.append_value(x);
                metrics.total_valid_values += 1;
            }
            ConversionResult::NullExplicite => {
                b.append_null();
                metrics.total_null_text += 1;
            }
            ConversionResult::ErreurConversion(raw) => {
                b.append_null();
                metrics.total_conversion_errors += 1;
                metrics.echantillon.add(raw);
            }
        }
    }
    Arc::new(b.finish())
}
pub fn build_bool_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    convertir: fn(&str) -> ConversionResult<bool>,
) -> ArrayRef {
    let mut b = BooleanBuilder::new();
    for v in values {
        match convertir(v) {
            ConversionResult::Valide(x) => {
                b.append_value(x);
                metrics.total_valid_values += 1;
            }
            ConversionResult::NullExplicite => {
                b.append_null();
                metrics.total_null_text += 1;
            }
            ConversionResult::ErreurConversion(raw) => {
                b.append_null();
                metrics.total_conversion_errors += 1;
                metrics.echantillon.add(raw);
            }
        }
    }
    Arc::new(b.finish())
}
pub fn build_float64_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    convertir: fn(&str) -> ConversionResult<f64>,
) -> ArrayRef {
    let mut b = Float64Builder::new();
    for v in values {
        match convertir(v) {
            ConversionResult::Valide(x) => {
                b.append_value(x);
                metrics.total_valid_values += 1;
            }
            ConversionResult::NullExplicite => {
                b.append_null();
                metrics.total_null_text += 1;
            }
            ConversionResult::ErreurConversion(raw) => {
                b.append_null();
                metrics.total_conversion_errors += 1;
                metrics.echantillon.add(raw);
            }
        }
    }
    Arc::new(b.finish())
}
pub fn build_date32_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    convertir: fn(&str) -> ConversionResult<i32>,
) -> ArrayRef {
    let mut b = Date32Builder::new();
    for v in values {
        match convertir(v) {
            ConversionResult::Valide(x) => {
                b.append_value(x);
                metrics.total_valid_values += 1;
            }
            ConversionResult::NullExplicite => {
                b.append_null();
                metrics.total_null_text += 1;
            }
            ConversionResult::ErreurConversion(raw) => {
                b.append_null();
                metrics.total_conversion_errors += 1;
                metrics.echantillon.add(raw);
            }
        }
    }
    Arc::new(b.finish())
}
pub fn build_timestamp_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    convertir: fn(&str) -> ConversionResult<i64>,
    unit: &TimeUnit,
) -> ArrayRef {
    match unit {
        TimeUnit::Second => {
            let mut b = TimestampSecondBuilder::new();
            for v in values {
                match convertir(v) {
                    ConversionResult::Valide(x) => {
                        b.append_value(x / 1000);
                        metrics.total_valid_values += 1;
                    }
                    ConversionResult::NullExplicite => {
                        b.append_null();
                        metrics.total_null_text += 1;
                    }
                    ConversionResult::ErreurConversion(raw) => {
                        b.append_null();
                        metrics.total_conversion_errors += 1;
                        metrics.echantillon.add(raw);
                    }
                }
            }
            Arc::new(b.finish())
        }
        TimeUnit::Millisecond => {
            let mut b = TimestampMillisecondBuilder::new();
            for v in values {
                match convertir(v) {
                    ConversionResult::Valide(x) => {
                        b.append_value(x);
                        metrics.total_valid_values += 1;
                    }
                    ConversionResult::NullExplicite => {
                        b.append_null();
                        metrics.total_null_text += 1;
                    }
                    ConversionResult::ErreurConversion(raw) => {
                        b.append_null();
                        metrics.total_conversion_errors += 1;
                        metrics.echantillon.add(raw);
                    }
                }
            }
            Arc::new(b.finish())
        }
        TimeUnit::Microsecond => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in values {
                match convertir(v) {
                    ConversionResult::Valide(x) => {
                        b.append_value(x * 1_000);
                        metrics.total_valid_values += 1;
                    }
                    ConversionResult::NullExplicite => {
                        b.append_null();
                        metrics.total_null_text += 1;
                    }
                    ConversionResult::ErreurConversion(raw) => {
                        b.append_null();
                        metrics.total_conversion_errors += 1;
                        metrics.echantillon.add(raw);
                    }
                }
            }
            Arc::new(b.finish())
        }
        TimeUnit::Nanosecond => {
            let mut b = TimestampNanosecondBuilder::new();
            for v in values {
                match convertir(v) {
                    ConversionResult::Valide(x) => {
                        b.append_value(x * 1_000_000);
                        metrics.total_valid_values += 1;
                    }
                    ConversionResult::NullExplicite => {
                        b.append_null();
                        metrics.total_null_text += 1;
                    }
                    ConversionResult::ErreurConversion(raw) => {
                        b.append_null();
                        metrics.total_conversion_errors += 1;
                        metrics.echantillon.add(raw);
                    }
                }
            }
            Arc::new(b.finish())
        }
    }
}
pub fn build_binary_column(values: &[String], metrics: &mut ColumnMetrics) -> ArrayRef {
    let mut b = BinaryBuilder::new();
    for v in values {
        if is_null_text(v) {
            b.append_null();
            metrics.total_null_text += 1;
        } else {
            b.append_value(v.as_bytes());
            metrics.total_valid_values += 1;
        }
    }
    Arc::new(b.finish())
}
pub fn build_large_binary_column(values: &[String], metrics: &mut ColumnMetrics) -> ArrayRef {
    let mut b = LargeBinaryBuilder::new();
    for v in values {
        if is_null_text(v) {
            b.append_null();
            metrics.total_null_text += 1;
        } else {
            b.append_value(v.as_bytes());
            metrics.total_valid_values += 1;
        }
    }
    Arc::new(b.finish())
}
pub fn build_utf8_column(values: &[String], metrics: &mut ColumnMetrics) -> ArrayRef {
    let mut b = StringBuilder::new();
    for v in values {
        if is_null_text(v) {
            b.append_null();
            metrics.total_null_text += 1;
        } else {
            b.append_value(v);
            metrics.total_valid_values += 1;
        }
    }
    Arc::new(b.finish())
}
pub fn build_large_utf8_column(
    values: &[String],
    metrics: &mut ColumnMetrics,
    force_utf8: bool,
) -> ArrayRef {
    let mut b = LargeStringBuilder::new();
    for v in values {
        if force_utf8 {
            b.append_value(v);
            metrics.total_valid_values += 1;
        } else if is_null_text(v) {
            b.append_null();
            metrics.total_null_text += 1;
        } else {
            b.append_value(v);
            metrics.total_valid_values += 1;
        }
    }
    Arc::new(b.finish())
}
pub fn build_default_column(values: &[String], metrics: &mut ColumnMetrics) -> ArrayRef {
    build_large_utf8_column(values, metrics, false)
}
// ============================================================
