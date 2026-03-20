// ============================================================
use crate::analysis::ErrorCounters;
use anyhow::Result;
use crossbeam::channel::Sender;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
pub fn produce_blocks<P: AsRef<Path>>(
    input_path: P,
    taille_bloc: usize,
    block_sender: &Sender<(usize, Vec<String>)>,
    counters: &Arc<ErrorCounters>,
    has_header: bool,
) -> Result<()> {
    let file = File::open(input_path)?;
    let mut reader = BufReader::new(file);
    if has_header {
        let mut header_line = String::new();
        reader.read_line(&mut header_line)?;
    }
    let mut block = Vec::with_capacity(taille_bloc);
    let mut block_index = 0usize;
    for resultat in reader.lines() {
        match resultat {
            Ok(row) => {
                block.push(row);
                if block.len() >= taille_bloc {
                    block_sender.send((block_index, block))?;
                    block = Vec::with_capacity(taille_bloc);
                    block_index += 1;
                }
            }
            Err(_) => {
                counters.raw_read_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    if !block.is_empty() {
        block_sender.send((block_index, block))?;
    }
    Ok(())
}
