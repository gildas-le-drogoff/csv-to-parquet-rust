// ============================================================
use crate::analysis::{analyze_block, BlockResult, ColumnMetrics, ErrorCounters};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use crossbeam::channel::{Receiver, Sender};
use rayon::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;
pub fn start_analysis_workers(
    block_receiver: Receiver<(usize, Vec<String>)>,
    batch_sender: Sender<(usize, RecordBatch)>,
    schema: Arc<arrow::datatypes::Schema>,
    delimiter: u8,
    global_metrics: Arc<Mutex<Vec<ColumnMetrics>>>,
    force_utf8: bool,
    counters: Arc<ErrorCounters>,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        block_receiver
            .into_iter()
            .par_bridge()
            .try_for_each(|(index, lines)| {
                let BlockResult { batch, metrics } =
                    analyze_block(&lines, schema.clone(), delimiter, force_utf8, &counters)?;
                {
                    let mut global = global_metrics.lock().unwrap();
                    for (i, m) in metrics.iter().enumerate() {
                        global[i].total_values += m.total_values;
                        global[i].total_null_text += m.total_null_text;
                        global[i].total_conversion_errors += m.total_conversion_errors;
                        global[i].total_valid_values += m.total_valid_values;
                        for v in &m.echantillon.values {
                            global[i].echantillon.add(v.clone());
                        }
                    }
                }
                batch_sender.send((index, batch)).unwrap();
                Ok(())
            })
    })
}
pub fn make_schema_all_nullable(schema: arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let fields: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|champ| arrow::datatypes::Field::new(champ.name(), champ.data_type().clone(), true))
        .collect();
    arrow::datatypes::Schema::new(fields)
}
// ============================================================
