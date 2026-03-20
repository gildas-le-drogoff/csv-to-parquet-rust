# 🦀 CSV to parquet

CSV/TSV → Parquet converter written in Rust. Automatic schema inference, ZSTD compression, parallel processing.

![Project demo](docs/csv-to-parquet.demo.gif)

## How it works

The program reads a csv file (CSV, TSV, or any delimiter among `,` `;` `\t` `|`), infers each column's type from a 10,000-row sample, then writes a compressed Parquet file.

### Inferred types

| Parquet type    | Detection conditions                                                 |
| --------------- | -------------------------------------------------------------------- |
| `Boolean`       | `true`/`false`, `yes`/`no`, `y`/`n`, `on`/`off`, `t`/`f`, `0`/`1`    |
| `Int64`         | Signed integers within the i64 range                                 |
| `UInt64`        | Positive integers exceeding i64 but within the u64 range             |
| `Float64`       | Numbers containing `.`, `e` or `E`                                   |
| `Date32`        | Formats `YYYY-MM-DD`, `DD/MM/YYYY`, `MM/DD/YYYY`                     |
| `Timestamp(ms)` | RFC3339, `YYYY-MM-DD HH:MM:SS`, variants with fractions and timezone |
| `LargeUtf8`     | Anything that doesn't match any of the above                         |

Values `null`, `NULL`, `None`, `NaN`, `N/A`, `na`, `nd`, `nr`, `-`, `--` and empty strings are treated as nulls regardless of the column type.

### Automatic detection

The delimiter is detected by counting occurrences across the first 20 lines. The header is detected through heuristics (comparing the first line's profile against the data). If no header is found, names `col_0`, `col_1`, ... are generated.

### Pipeline

```
CSV file
    │
    ├─ Delimiter detection
    ├─ Header detection
    ├─ Schema inference (10,000-row sample)
    │
    ├─ Reading in 100,000-line blocks
    ├─ Parallel conversion (rayon + crossbeam)
    ├─ Ordered Parquet writing (ZSTD level 5)
    │
    └─ Validation report (row consistency, null/error rates per column)
```

## Installation

```bash
cargo build --release
```

The binary is located at `target/release/csv_to_parquet`.

## Usage

### Simple conversion

```bash
csv_to_parquet file.csv
```

Produces `file.parquet` in the same directory.

### TSV or other delimiters

```bash
csv_to_parquet file.tsv
```

The delimiter is detected automatically.

### From stdin

```bash
cat file.csv | csv_to_parquet -
```

Produces `stdin.parquet` in the current directory.

### Options

```
--inferer-schema-complet   Analyze the entire file for inference
                           (slower but more accurate for files where
                           types vary beyond the first 10,000 rows)

--force-utf8               Force all columns to LargeUtf8
                           (disables all inference, preserves raw data)

--view-schema              Display the logical and physical schema of a Parquet file

--man                      Generate the man page in roff format
```

### Examples

```bash
# Conversion with extended inference
csv_to_parquet --inferer-schema-complet large_file.csv

# Inspect the result
csv_to_parquet --view-schema large_file.parquet

# Everything as text (no possible semantic loss)
csv_to_parquet --force-utf8 messy_data.csv

# Man page
csv_to_parquet --man > csv_to_parquet.1
man ./csv_to_parquet.1
```

## Output report

Each conversion produces a report on stderr:

```
========== VALIDATION REPORT ==========

CSV rows           1000000
Parquet rows       1000000
Parsing errors           0
Read errors              0
Total errors             0
[OK] Consistency validated

========== COLUMNS ==========

name                     type           null %      err %    valid %     conf
--------------------------------------------------------------------------------------
id                       Int64           0.00        0.00     100.00   100.00
price                    Float64         0.50        0.00      99.50    99.50
sale_date                Date32          1.20        0.00      98.80    98.80
description              LargeUtf8       0.00        0.00     100.00   100.00
```

The `err %` column indicates the percentage of non-null values that could not be converted to the inferred type. These values are replaced by nulls in the Parquet output.

## Tests

```bash
cargo test
```

The test suite covers schema inference, type conversions, null and error handling, strict block write ordering, the full pipeline, and delimiter detection.

A Python script allows testing against generated data:

```bash
python3 test_csv_to_parquet.py
```

Prerequisite: `pyarrow` (`pip install pyarrow`).

## Demo

```bash
chmod +x demo.sh
asciinema rec demo.cast \
    --overwrite \
    --title "csv to parquet" \
    --cols 120 \
    --rows 34 \
    --command "bash demo.sh"
```

## Known limitations

- Inference is strict: a single non-conforming value invalidates the type for the entire column. A column with 99.9% integers and one text value will be inferred as `LargeUtf8`.
- Ambiguous date formats (`01/02/2024`: January 2nd or February 1st?) are tested in order `DD/MM/YYYY` then `MM/DD/YYYY`. The first successful parse wins. If both are valid, `DD/MM/YYYY` is retained.
- Header detection is heuristic-based. A false positive is possible when the first data row structurally resembles a header (short, unique, alphabetic values).
- No support for compressed input files (gzip, bzip2). Decompress first or use stdin: `zcat file.csv.gz | csv_to_parquet -`.

## Dependencies

| Crate                    | Role                             |
| ------------------------ | -------------------------------- |
| `arrow` / `parquet`      | Arrow schema, Parquet writing    |
| `csv`                    | CSV reading                      |
| `chrono`                 | Date and timestamp parsing       |
| `rayon`                  | Block-level parallelism          |
| `crossbeam`              | Bounded channels between threads |
| `clap`                   | Command-line interface           |
| `indicatif`              | Progress bar                     |
| `anyhow`                 | Error handling                   |
| `lexical-core`           | Fast numeric parsing             |
| `owo-colors` / `colored` | Terminal colors                  |
| `tempfile`               | Temporary files (stdin)          |

## License

Unspecified.
