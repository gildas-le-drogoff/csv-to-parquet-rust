// ============================================================
use arrow::record_batch::RecordBatch;
#[derive(Clone, Debug)]
pub struct ErrorSample {
    pub values: Vec<String>,
    pub limite: usize,
}
impl ErrorSample {
    pub fn new(limite: usize) -> Self {
        Self {
            values: Vec::new(),
            limite,
        }
    }
    pub fn add(&mut self, value: String) {
        if self.values.len() < self.limite {
            self.values.push(value);
        }
    }
}
#[derive(Clone, Debug)]
pub struct ColumnMetrics {
    pub name: String,
    pub total_values: usize,
    pub total_null_text: usize,
    pub total_conversion_errors: usize,
    pub total_valid_values: usize,
    pub echantillon: ErrorSample,
}
impl ColumnMetrics {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            total_values: 0,
            total_null_text: 0,
            total_conversion_errors: 0,
            total_valid_values: 0,
            echantillon: ErrorSample::new(10),
        }
    }
}
pub enum ConversionResult<T> {
    Valide(T),
    NullExplicite,
    ErreurConversion(String),
}
pub struct BlockResult {
    pub batch: RecordBatch,
    pub metrics: Vec<ColumnMetrics>,
}
#[derive(Default)]
pub struct ErrorCounters {
    pub parse_errors: std::sync::atomic::AtomicUsize,
    pub erreurs_structure_csv: std::sync::atomic::AtomicUsize,
    pub raw_read_errors: std::sync::atomic::AtomicUsize,
}
// ============================================================
