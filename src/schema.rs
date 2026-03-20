// ============================================================
use crate::utils::{generate_column_names, is_null_text, parse_bool};
use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use colored::Colorize;
use csv::ReaderBuilder;
use log::{debug, info};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
const BLOCK_SAMPLE_SIZE: usize = 10_000;
#[derive(Clone, Copy, PartialEq, Eq)]
enum TimestampFormat {
    Textuel,
    Numerique,
}
#[derive(Clone)]
struct TypeState {
    peut_bool: bool,
    peut_date: bool,
    peut_timestamp: bool,
    peut_int: bool,
    peut_uint: bool,
    peut_float: bool,
    min_i128: i128,
    max_i128: i128,
    float_detecte: bool,
    ts_s: u64,
    ts_ms: u64,
    ts_us: u64,
    ts_ns: u64,
    ts_textuel: u64,
    total_values: u64,
}
impl TypeState {
    fn new() -> Self {
        Self {
            peut_bool: true,
            peut_date: true,
            peut_timestamp: true,
            peut_int: true,
            peut_uint: true,
            peut_float: true,
            min_i128: i128::MAX,
            max_i128: i128::MIN,
            float_detecte: false,
            ts_s: 0,
            ts_ms: 0,
            ts_us: 0,
            ts_ns: 0,
            ts_textuel: 0,
            total_values: 0,
        }
    }
    fn observe(&mut self, value: &str) {
        if is_null_text(value) {
            return;
        }
        let t = value.trim();
        self.total_values += 1;
        if self.peut_bool && parse_bool(t).is_none() {
            self.peut_bool = false;
        }
        if self.peut_int || self.peut_uint {
            match t.parse::<i128>() {
                Ok(v) => {
                    self.min_i128 = self.min_i128.min(v);
                    self.max_i128 = self.max_i128.max(v);
                    if v < 0 {
                        self.peut_uint = false;
                    }
                }
                Err(_) => {
                    self.peut_int = false;
                    self.peut_uint = false;
                }
            }
        }
        if self.peut_float {
            match t.parse::<f64>() {
                Ok(_) => {
                    if t.contains('.') || t.contains('e') || t.contains('E') {
                        self.float_detecte = true;
                    }
                }
                Err(_) => {
                    self.peut_float = false;
                }
            }
        }
        if self.peut_date {
            let date_ok = NaiveDate::parse_from_str(t, "%Y-%m-%d").is_ok()
                || NaiveDate::parse_from_str(t, "%d/%m/%Y").is_ok()
                || NaiveDate::parse_from_str(t, "%m/%d/%Y").is_ok();
            if !date_ok {
                self.peut_date = false;
            }
        }
        if self.peut_timestamp {
            match detect_timestamp_unit(t) {
                Some((unit, forme)) => {
                    if forme == TimestampFormat::Textuel {
                        self.ts_textuel += 1;
                    }
                    match unit {
                        TimeUnit::Second => self.ts_s += 1,
                        TimeUnit::Millisecond => self.ts_ms += 1,
                        TimeUnit::Microsecond => self.ts_us += 1,
                        TimeUnit::Nanosecond => self.ts_ns += 1,
                    }
                }
                None => {
                    self.peut_timestamp = false;
                }
            }
        }
    }
    fn choose_timestamp_unit(&self) -> TimeUnit {
        let candidates = [
            (TimeUnit::Second, self.ts_s),
            (TimeUnit::Millisecond, self.ts_ms),
            (TimeUnit::Microsecond, self.ts_us),
            (TimeUnit::Nanosecond, self.ts_ns),
        ];
        candidates
            .into_iter()
            .max_by_key(|(_, c)| *c)
            .map(|(u, _)| u)
            .unwrap_or(TimeUnit::Millisecond)
    }
    fn finalize(&self) -> DataType {
        if self.total_values == 0 {
            return DataType::LargeUtf8;
        }
        if self.peut_timestamp {
            let total_ts = self.ts_s + self.ts_ms + self.ts_us + self.ts_ns;
            let total_values = self.total_values.max(1);
            let confidence = total_ts as f64 / total_values as f64;
            let also_integer = self.peut_int || self.peut_uint;
            let has_textual_format = self.ts_textuel > 0;
            if total_ts > 0 && confidence >= 0.80 && !(also_integer && !has_textual_format) {
                let unit = self.choose_timestamp_unit();
                return DataType::Timestamp(unit, None);
            }
        }
        if self.peut_date {
            return DataType::Date32;
        }
        if self.peut_bool {
            return DataType::Boolean;
        }
        if self.peut_int {
            if self.min_i128 >= 0 && self.max_i128 <= u64::MAX as i128 {
                if self.max_i128 > i64::MAX as i128 {
                    return DataType::UInt64;
                }
            }
            if self.min_i128 >= i64::MIN as i128 && self.max_i128 <= i64::MAX as i128 {
                return DataType::Int64;
            }
        }
        if self.peut_uint {
            if self.min_i128 >= 0 && self.max_i128 <= u64::MAX as i128 {
                return DataType::UInt64;
            }
        }
        if self.peut_float {
            return DataType::Float64;
        }
        DataType::LargeUtf8
    }
}
fn detect_timestamp_unit(value: &str) -> Option<(TimeUnit, TimestampFormat)> {
    let v = value.trim();
    if chrono::DateTime::parse_from_rfc3339(v).is_ok() {
        return Some((TimeUnit::Millisecond, TimestampFormat::Textuel));
    }
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ];
    for f in formats {
        if NaiveDateTime::parse_from_str(v, f).is_ok() {
            return Some((TimeUnit::Millisecond, TimestampFormat::Textuel));
        }
    }
    if let Ok(x) = v.parse::<i128>() {
        let abs = x.abs();
        if abs < 100_000_000_000 {
            return Some((TimeUnit::Second, TimestampFormat::Numerique));
        }
        if abs < 100_000_000_000_000 {
            return Some((TimeUnit::Millisecond, TimestampFormat::Numerique));
        }
        if abs < 100_000_000_000_000_000 {
            return Some((TimeUnit::Microsecond, TimestampFormat::Numerique));
        }
        return Some((TimeUnit::Nanosecond, TimestampFormat::Numerique));
    }
    None
}
pub fn infer_schema<P: AsRef<Path>>(
    path: P,
    delimiter: u8,
    _analyser_tout: bool,
    has_header: bool,
) -> Result<Schema> {
    let path_ref = path.as_ref();
    let mut file = BufReader::new(
        File::open(path_ref)
            .with_context(|| format!("Impossible d'ouvrir {}", path_ref.display()))?,
    );
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(true)
        .from_reader(&mut file);
    let column_names: Vec<String> = if has_header {
        reader.headers()?.iter().map(|s| s.to_string()).collect()
    } else {
        let first = reader
            .records()
            .next()
            .transpose()?
            .context("Fichier vide")?;
        generate_column_names(first.len())
    };
    let column_count = column_names.len();
    info!("{} {}", "[INFÉRENCE] Colonnes".blue(), column_count);
    if !has_header {
        info!(
            "{} {}",
            "[INFÉRENCE] Pas d'en-tête détecté, noms générés :".yellow(),
            column_names.join(", ")
        );
    }
    drop(reader);
    file.seek(SeekFrom::Start(0))?;
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(true)
        .from_reader(&mut file);
    let mut states: Vec<TypeState> = (0..column_count).map(|_| TypeState::new()).collect();
    for (i, record) in reader.records().enumerate() {
        if i >= BLOCK_SAMPLE_SIZE {
            debug!(
                "{} échantillonnage arrêté à {} lines",
                "[INFÉRENCE]", BLOCK_SAMPLE_SIZE
            );
            break;
        }
        let r = match record {
            Ok(v) => v,
            Err(_) => continue,
        };
        for (col, val) in r.iter().enumerate() {
            if let Some(etat) = states.get_mut(col) {
                etat.observe(val);
            }
        }
    }
    let fields: Vec<Field> = column_names
        .iter()
        .zip(states.iter())
        .map(|(name, etat)| {
            let dtype = etat.finalize();
            info!(
                "{} '{}' -> {:?}",
                "[TYPE FINAL]".green().bold(),
                name,
                dtype
            );
            Field::new(name, dtype, true)
        })
        .collect();
    Ok(Schema::new(fields))
}
pub fn force_schema_to_utf8(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), DataType::LargeUtf8, true))
        .collect();
    Schema::new(fields)
}
