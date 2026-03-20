// ============================================================
use crate::analysis::{ColumnMetrics, ErrorCounters};
use crate::conversion::pipeline::start_analysis_workers;
use crate::schema::{force_schema_to_utf8, infer_schema};
use crate::utils::{detect_delimiter, detect_header};
use crate::BLOCK_SIZE;
use anyhow::{Context, Result};
use colored::Colorize;
use counting::{count_lines, count_parquet_lines};
use csv_blocks::produce_blocks;
use indicatif::{ProgressBar, ProgressStyle};
pub use parquet_writer::{start_parquet_writer, verify_parquet_schema};
use pipeline::make_schema_all_nullable;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use ticker::start_ticker;
mod counting;
mod csv_blocks;
mod parquet_writer;
mod pipeline;
mod ticker;
const BLOCK_QUEUE_CAPACITY: usize = 8;
const BATCH_QUEUE_CAPACITY: usize = 8;
const THROUGHPUT_WINDOW: Duration = Duration::from_secs(2);
pub fn convert_csv_to_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    input_path: P,
    output_path: Q,
    full_schema_inference: bool,
    force_utf8: bool,
) -> Result<()> {
    let start_instant = Instant::now();
    let counters = Arc::new(ErrorCounters::default());
    eprintln!("{} {}", "[INFO]".blue().bold(), "Opening input file".blue());
    let delimiter = detect_delimiter(&input_path)?;
    let has_header = detect_header(&input_path, delimiter)?;
    if !has_header {
        eprintln!(
            "{} {}",
            "[INFO]".yellow().bold(),
            "No header detected, column names generated automatically (col_0, col_1, ...)".yellow()
        );
    }
    eprintln!("{} {}", "[PHASE]".cyan().bold(), "Schema inference".cyan());
    let initial_schema = infer_schema(&input_path, delimiter, full_schema_inference, has_header)?;
    let effective_schema = if force_utf8 {
        force_schema_to_utf8(&initial_schema)
    } else {
        initial_schema.clone()
    };
    let nullable_schema = make_schema_all_nullable(effective_schema.clone());
    let schema_arc = Arc::new(nullable_schema.clone());
    let column_count = schema_arc.fields().len();
    eprintln!(
        "{} {} {}",
        "[OK]".green().bold(),
        "Schema detected:".green(),
        column_count.to_string().green().bold()
    );
    let total_csv_lines = count_lines(&input_path, delimiter, has_header)?;
    let progress_bar = ProgressBar::new(total_csv_lines as u64);
    progress_bar.set_style(
        ProgressStyle::with_template(
            "{elapsed_precise} [{bar:40.cyan/blue}] {pos}/{len} ETA {eta}",
        )?
        .progress_chars("█░"),
    );
    let (block_sender, block_receiver) =
        crossbeam::channel::bounded::<(usize, Vec<String>)>(BLOCK_QUEUE_CAPACITY);
    let (batch_sender, batch_receiver) = crossbeam::channel::bounded::<(
        usize,
        arrow::record_batch::RecordBatch,
    )>(BATCH_QUEUE_CAPACITY);
    let global_metrics = Arc::new(Mutex::new(
        schema_arc
            .fields()
            .iter()
            .map(|f| ColumnMetrics::new(f.name()))
            .collect::<Vec<_>>(),
    ));
    let analysis_handle = start_analysis_workers(
        block_receiver,
        batch_sender.clone(),
        schema_arc.clone(),
        delimiter,
        global_metrics.clone(),
        force_utf8,
        counters.clone(),
    );
    let writer_handle = start_parquet_writer(
        batch_receiver,
        &output_path,
        schema_arc.clone(),
        BLOCK_SIZE,
        progress_bar.clone(),
    )?;
    let ticker_handle = start_ticker(progress_bar.clone());
    produce_blocks(
        &input_path,
        BLOCK_SIZE,
        &block_sender,
        &counters,
        has_header,
    )?;
    drop(block_sender);
    analysis_handle.join().expect("Analysis interrupted")?;
    drop(batch_sender);
    writer_handle.join().unwrap()?;
    progress_bar.finish_with_message("Write complete");
    ticker_handle.join().ok();
    let duration = start_instant.elapsed();
    let csv_parsing_errors = counters.parse_errors.load(Ordering::Relaxed);
    let read_errors = counters.raw_read_errors.load(Ordering::Relaxed);
    let csv_errors = csv_parsing_errors + read_errors;
    let total_parquet_lines = count_parquet_lines(&output_path)?;
    eprintln!(
        "\n{}\n",
        "========== VALIDATION REPORT ==========".magenta().bold()
    );
    eprintln!(
        "{} {:>12}\n{} {:>12}\n{} {:>12}\n{} {:>12}\n{} {:>12}",
        "CSV lines".blue(),
        total_csv_lines.to_string().blue().bold(),
        "Parquet lines".blue(),
        total_parquet_lines.to_string().blue().bold(),
        "Parse errors".yellow(),
        csv_parsing_errors.to_string().yellow().bold(),
        "Read errors".yellow(),
        read_errors.to_string().yellow().bold(),
        "Total errors".red(),
        csv_errors.to_string().red().bold(),
    );
    if total_parquet_lines != total_csv_lines {
        eprintln!(
            "{} {}",
            "[WARN]".yellow().bold(),
            format!(
                "delta={}",
                total_csv_lines as i64 - total_parquet_lines as i64
            )
            .yellow()
        );
    } else {
        eprintln!("{} Consistency validated", "[OK]".green().bold());
    }
    display_metrics_table(&schema_arc, &global_metrics.lock().unwrap());
    eprintln!("\n{} {:.2?}", "Duration".green(), duration);
    verify_parquet_schema(&output_path).context("Invalid Parquet schema")?;
    Ok(())
}
fn display_metrics_table(schema: &arrow::datatypes::Schema, metrics: &[ColumnMetrics]) {
    eprintln!("\n{}\n", "========== COLUMNS ==========".magenta().bold());
    eprintln!(
        "{:<24} {:<12} {:>12} {:>12} {:>12} {:>10}",
        "name", "type", "null %", "err %", "valid %", "conf"
    );
    eprintln!("{}", "-".repeat(86));
    for (i, m) in metrics.iter().enumerate() {
        let total = m.total_values.max(1) as f64;
        let null_rate = m.total_null_text as f64 / total * 100.0;
        let error_rate = m.total_conversion_errors as f64 / total * 100.0;
        let valid_rate = m.total_valid_values as f64 / total * 100.0;
        let final_type = format!("{:?}", schema.fields()[i].data_type());
        eprintln!(
            "{:<24} {:<12} {:>11.2} {:>11.2} {:>11.2} {:>9.2}",
            m.name, final_type, null_rate, error_rate, valid_rate, valid_rate
        );
    }
    eprintln!(
        "\n{}\n",
        "================================".magenta().bold()
    );
}
