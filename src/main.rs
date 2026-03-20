// ============================================================
use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use clap_mangen::Man;
use csv_to_parquet::conversion::{convert_csv_to_parquet, verify_parquet_schema};
use csv_to_parquet::utils::{error, path, success, warning};
use log::info;
use std::fs::File;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
#[derive(Parser, Debug)]
#[command(
    name = "csv_to_parquet",
    version,
    about = "Convert CSV/TSV to Parquet",
    long_about = r#"Convert a CSV/TSV file to Parquet.
Features:
- Automatic delimiter detection.
- Schema inference (int, float, bool, date, timestamp).
- Compressed Parquet output (ZSTD).
- Post-write validation.
Options:
  --full-schema-inference    Full analysis for more accurate inference.
  --view-schema              Display the schema of a Parquet file.
  --force-utf8               Force all columns to text.
  --man                      Generate the man page (stdout or file).
Examples:
  csv_to_parquet file.csv
  csv_to_parquet --full-schema-inference file.csv
  csv_to_parquet --view-schema file.parquet
  csv_to_parquet --man > csv_to_parquet.1
  cat file.csv | csv_to_parquet -
Exit codes:
  0  Success
  1  Error"#
)]
struct CommandInterface {
    #[arg(
        long,
        help = "Analyze the entire file for inference (slower but more accurate)"
    )]
    full_schema_inference: bool,
    #[arg(
        long,
        help = "Display the logical and physical schema of a Parquet file"
    )]
    view_schema: bool,
    #[arg(
        long,
        help = "Force all columns to Utf8 (disables all semantic inference)"
    )]
    force_utf8: bool,
    #[arg(long, help = "Generate the man page (stdout if no file provided)")]
    man: bool,
    #[arg(value_name = "INPUT", help = "Input CSV/TSV file or '-' for stdin")]
    input: Option<String>,
}
fn main() {
    if let Err(e) = execute() {
        eprintln!("{} {}", error("Error:"), e);
        std::process::exit(1);
    }
}
fn execute() -> Result<()> {
    let cli = CommandInterface::parse();
    if cli.man {
        generate_manpage(None)?;
        return Ok(());
    }
    if cli.view_schema {
        let file_path = cli
            .input
            .as_ref()
            .context("--view-schema requires a Parquet file as input")?;
        verify_parquet_schema(file_path)?;
        return Ok(());
    }
    let (input_path, output_path) = match cli.input.as_deref() {
        Some("-") => {
            if io::stdin().is_terminal() {
                display_help();
                anyhow::bail!("Stdin requested but no stream is redirected");
            }
            let temporary_path = write_stdin_to_temp_file()?;
            (temporary_path, PathBuf::from("stdin.parquet"))
        }
        Some(file) => {
            let output = build_parquet_output_path(file);
            (PathBuf::from(file), output)
        }
        None => {
            display_help();
            anyhow::bail!("No input provided");
        }
    };
    if cli.force_utf8 {
        eprintln!(
            "{}",
            warning(
                "Option --force-utf8 active: all columns will be converted to Utf8 text. \
Result is usable but semantically degraded (numeric types, dates, timestamps are lost)."
            )
        );
    }
    convert_csv_to_parquet(
        input_path.clone(),
        output_path.clone(),
        cli.full_schema_inference,
        cli.force_utf8,
    )
    .with_context(|| {
        format!(
            "Conversion failed {} -> {}",
            path(&input_path),
            path(&output_path)
        )
    })?;
    eprintln!("{} {}", success("Conversion complete:"), path(&output_path));
    Ok(())
}
fn generate_manpage(output: Option<PathBuf>) -> Result<()> {
    let mut cmd = CommandInterface::command();
    cmd = cmd.name("csv_to_parquet");
    let man = Man::new(cmd);
    match output {
        Some(path) => {
            let mut file = File::create(path)?;
            man.render(&mut file)?;
        }
        None => {
            let mut stdout = io::stdout();
            man.render(&mut stdout)?;
        }
    }
    Ok(())
}
fn display_help() {
    let mut command = CommandInterface::command();
    let _ = command.print_help();
    eprintln!();
}
fn write_stdin_to_temp_file() -> Result<PathBuf> {
    let mut buffer = Vec::new();
    io::stdin().read_to_end(&mut buffer)?;
    if buffer.is_empty() {
        display_help();
        eprintln!("{}", warning("Empty stdin"));
        anyhow::bail!("Empty stdin");
    }
    let mut temp_file = NamedTempFile::new()?;
    temp_file.write_all(&buffer)?;
    let (_file, file_path) = temp_file.keep()?;
    info!("Stdin written to {:?}", file_path);
    Ok(file_path)
}
fn build_parquet_output_path(input: &str) -> PathBuf {
    let input_path = Path::new(input);
    let mut output_directory = input_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let base_name = input_path.file_stem().unwrap_or_default().to_string_lossy();
    output_directory.push(format!("{base_name}.parquet"));
    output_directory
}
