// ============================================================
// src/utils.rs
use anyhow::Result;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use colored::Colorize;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal};
use std::path::Path;
fn colors_enabled() -> bool {
    io::stdout().is_terminal() && io::stderr().is_terminal()
}
pub fn error(msg: impl std::fmt::Display) -> String {
    let s = msg.to_string();
    if colors_enabled() {
        s.red().bold().to_string()
    } else {
        s
    }
}
pub fn warning(msg: impl std::fmt::Display) -> String {
    let s = msg.to_string();
    if colors_enabled() {
        s.yellow().to_string()
    } else {
        s
    }
}
pub fn success(msg: impl std::fmt::Display) -> String {
    let s = msg.to_string();
    if colors_enabled() {
        s.green().to_string()
    } else {
        s
    }
}
pub fn path(p: &Path) -> String {
    let s = p.display().to_string();
    if colors_enabled() {
        s.cyan().to_string()
    } else {
        s
    }
}
pub fn is_null_text(v: &str) -> bool {
    let t = v.trim();
    if t.is_empty() {
        return true;
    }
    matches!(
        t.to_ascii_lowercase().as_str(),
        "null" | "none" | "nan" | "n/a" | "na" | "nd" | "nr" | "-" | "--"
    )
}
pub fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "t" | "y" | "yes" | "on" => Some(true),
        "false" | "0" | "f" | "n" | "no" | "off" => Some(false),
        _ => None,
    }
}
pub fn parse_date_ymd(v: &str) -> Option<i32> {
    let t = v.trim();
    if t.is_empty() {
        return None;
    }
    let date = NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(t, "%d/%m/%Y"))
        .or_else(|_| NaiveDate::parse_from_str(t, "%m/%d/%Y"))
        .ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let days = (date - epoch).num_days();
    i32::try_from(days).ok()
}
pub fn parse_timestamp_ms(v: &str) -> Option<i64> {
    let t = v.trim();
    if t.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(t) {
        return Some(dt.timestamp_millis());
    }
    let timezone_formats = [
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%.f%z",
    ];
    for f in timezone_formats {
        if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(t, f) {
            return Some(dt.timestamp_millis());
        }
    }
    let naive_formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ];
    for f in naive_formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(t, f) {
            let utc_datetime = Utc.from_utc_datetime(&dt);
            return Some(utc_datetime.timestamp_millis());
        }
    }
    if let Ok(x) = t.parse::<i128>() {
        if (1_000_000_000..4_000_000_000).contains(&x) {
            return Some((x as i64) * 1000);
        }
        if (1_000_000_000_000..4_000_000_000_000).contains(&x) {
            return Some(x as i64);
        }
        if (1_000_000_000_000_000..4_000_000_000_000_000).contains(&x) {
            return Some((x / 1000) as i64);
        }
        if x >= 1_000_000_000_000_000_000 {
            return Some((x / 1_000_000) as i64);
        }
    }
    None
}
pub fn detect_delimiter<P: AsRef<Path>>(path: P) -> Result<u8> {
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let candidates = [',', ';', '\t', '|'];
    let mut scores = vec![0usize; candidates.len()];
    for row in reader.lines().take(20) {
        let row = row?;
        for (i, c) in candidates.iter().enumerate() {
            scores[i] += row.matches(*c).count();
        }
    }
    let (index, _) = scores.iter().enumerate().max_by_key(|(_, v)| *v).unwrap();
    Ok(candidates[index] as u8)
}
fn value_type_score(v: &str) -> f64 {
    let t = v.trim();
    if t.is_empty() {
        return 0.0;
    }
    if t.parse::<i64>().is_ok() {
        return 1.0;
    }
    if t.parse::<f64>().is_ok() {
        return 1.0;
    }
    if parse_bool(t).is_some() {
        return 0.8;
    }
    if parse_date_ymd(t).is_some() {
        return 1.0;
    }
    if parse_timestamp_ms(t).is_some() {
        return 1.0;
    }
    0.0
}
fn looks_like_identifier(v: &str) -> bool {
    let t = v.trim();
    if t.is_empty() || t.len() > 64 {
        return false;
    }
    t.chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ' ')
}
pub fn detect_header<P: AsRef<Path>>(path: P, delimiter: u8) -> Result<bool> {
    let file = File::open(&path)?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::new(file));
    let mut records = reader.records();
    let first_line = match records.next() {
        Some(Ok(r)) => r,
        _ => return Ok(false),
    };
    let column_count = first_line.len();
    if column_count == 0 {
        return Ok(false);
    }
    let first_values: Vec<String> = first_line.iter().map(|s| s.to_string()).collect();
    let first_score: f64 = first_values
        .iter()
        .map(|v| value_type_score(v))
        .sum::<f64>()
        / column_count as f64;
    let max_sample = 200;
    let mut data_score = 0.0;
    let mut data_line_count = 0usize;
    let mut data_length = 0.0f64;
    let mut first_line_repetitions = 0usize;
    for record in records.take(max_sample) {
        let r = match record {
            Ok(r) => r,
            Err(_) => continue,
        };
        let s: f64 = r.iter().map(value_type_score).sum::<f64>() / column_count.max(1) as f64;
        data_score += s;
        let line_length: usize = r.iter().map(|v| v.len()).sum();
        data_length += line_length as f64;
        let first_match = r
            .iter()
            .zip(first_values.iter())
            .any(|(v, p)| !p.is_empty() && v == p.as_str());
        if first_match {
            first_line_repetitions += 1;
        }
        data_line_count += 1;
    }
    if data_line_count == 0 {
        return Ok(true);
    }
    let average_data_score = data_score / data_line_count as f64;
    if first_score < average_data_score - 0.2 {
        return Ok(true);
    }
    let all_identifiers = first_values.iter().all(|v| looks_like_identifier(v));
    let mut unique_values = first_values.clone();
    unique_values.sort();
    unique_values.dedup();
    let all_unique = unique_values.len() == column_count;
    let first_length: usize = first_values.iter().map(|v| v.len()).sum();
    let average_data_length = data_length / data_line_count as f64;
    let first_is_shorter = (first_length as f64) < average_data_length * 0.7;
    let repetition_rate = first_line_repetitions as f64 / data_line_count as f64;
    let few_repetitions = repetition_rate < 0.05;
    if all_identifiers && all_unique && first_is_shorter && few_repetitions {
        return Ok(true);
    }
    Ok(false)
}
pub fn generate_column_names(nb: usize) -> Vec<String> {
    (0..nb).map(|i| format!("col_{i}")).collect()
}
