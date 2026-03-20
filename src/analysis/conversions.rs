// ============================================================
use super::types::ConversionResult;
use crate::utils::{is_null_text, parse_bool, parse_date_ymd, parse_timestamp_ms};
pub fn convert_to_i64(v: &str) -> ConversionResult<i64> {
    if is_null_text(v) {
        return ConversionResult::NullExplicite;
    }
    match lexical_core::parse::<i64>(v.trim().as_bytes()) {
        Ok(x) => ConversionResult::Valide(x),
        Err(_) => ConversionResult::ErreurConversion(v.to_string()),
    }
}
pub fn convert_to_u64(v: &str) -> ConversionResult<u64> {
    if is_null_text(v) {
        return ConversionResult::NullExplicite;
    }
    match lexical_core::parse::<u64>(v.trim().as_bytes()) {
        Ok(x) => ConversionResult::Valide(x),
        Err(_) => ConversionResult::ErreurConversion(v.to_string()),
    }
}
pub fn convert_to_bool(v: &str) -> ConversionResult<bool> {
    if is_null_text(v) {
        return ConversionResult::NullExplicite;
    }
    match parse_bool(v) {
        Some(b) => ConversionResult::Valide(b),
        None => ConversionResult::ErreurConversion(v.to_string()),
    }
}
pub fn convert_to_f64(v: &str) -> ConversionResult<f64> {
    if is_null_text(v) {
        return ConversionResult::NullExplicite;
    }
    match lexical_core::parse::<f64>(v.trim().as_bytes()) {
        Ok(x) => ConversionResult::Valide(x),
        Err(_) => ConversionResult::ErreurConversion(v.to_string()),
    }
}
pub fn convert_to_date32(v: &str) -> ConversionResult<i32> {
    if is_null_text(v) {
        return ConversionResult::NullExplicite;
    }
    match parse_date_ymd(v) {
        Some(days) => ConversionResult::Valide(days),
        None => ConversionResult::ErreurConversion(v.to_string()),
    }
}
pub fn convert_to_timestamp_ms(v: &str) -> ConversionResult<i64> {
    if is_null_text(v) {
        return ConversionResult::NullExplicite;
    }
    match parse_timestamp_ms(v) {
        Some(ms) => ConversionResult::Valide(ms),
        None => ConversionResult::ErreurConversion(v.to_string()),
    }
}
// ============================================================
