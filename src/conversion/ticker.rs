// ============================================================
use crate::conversion::THROUGHPUT_WINDOW;
use indicatif::ProgressBar;
use std::collections::VecDeque;
use std::thread;
use std::time::{Duration, Instant};
pub fn start_ticker(progress_bar: ProgressBar) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut history: VecDeque<(Instant, u64)> = VecDeque::new();
        loop {
            thread::sleep(Duration::from_millis(200));
            let now = Instant::now();
            let position = progress_bar.position();
            history.push_back((now, position));
            while let Some((instant, _)) = history.front() {
                if now.duration_since(*instant) > THROUGHPUT_WINDOW {
                    history.pop_front();
                } else {
                    break;
                }
            }
            if history.len() >= 2 {
                let (start_time, start_lines) = history.front().unwrap();
                let (end_time, end_lines) = history.back().unwrap();
                let time_delta = end_time.duration_since(*start_time).as_secs_f64();
                if time_delta > 0.0 && end_lines > start_lines {
                    let throughput = ((end_lines - start_lines) as f64 / time_delta)
                        .round()
                        .max(1.0) as u64;
                    progress_bar.set_message(format!("{throughput} l/s"));
                }
            }
            if progress_bar.is_finished() {
                break;
            }
        }
    })
}
// ============================================================
