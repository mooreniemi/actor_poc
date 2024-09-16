use actix::{Actor, ActorContext, Addr, Context, Handler};
use log::info;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::{coordinator::Coordinator, messages::ProcessMessage, step::Step};

/// CsvReader Actor
///
/// Responsible for reading feature vectors from a CSV file and sending them to the Coordinator.
pub struct CsvReader {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub file_path: String,
    pub interval_secs: u64,
    pub current_count: u32,
    pub next_id: u64,
    pub send_batch_info: bool,
    pub batch_size: Option<u32>,
    pub current_batch_id: u64,
    pub current_batch_count: u32,
    pub params: Value,
}

impl Step for CsvReader {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        let file_path = params
            .get("file_path")
            .and_then(|v| v.as_str()) // Extract the file path as a string
            .unwrap_or_default() // Default to an empty string if not found
            .to_string();

        let batch_mode = params
            .get("batch_mode")
            .and_then(|v| v.as_bool()) // Extract the value as a boolean
            .unwrap_or(false); // Default to `false` if not found

        let batch_size = params
            .get("batch_size")
            .and_then(|v| v.as_u64()) // Extract the value as a u64 if it's a valid number
            .map(|v| v as u32); // Convert to u32

        CsvReader {
            name,
            output_name,
            coordinator,
            file_path,
            interval_secs: 1, // Default interval
            current_count: 0,
            next_id: 1,
            send_batch_info: batch_mode,
            batch_size,
            current_batch_id: 1,
            current_batch_count: 0,
            params,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_name(&self) -> &str {
        &self.output_name
    }

    fn coordinator(&self) -> Addr<Coordinator> {
        self.coordinator.clone()
    }

    fn params(&self) -> &Value {
        &self.params
    }
}

impl Actor for CsvReader {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!(
            "CsvReader '{}' started reading from file: {}",
            self.name, self.file_path
        );
        self.read_and_send_data(ctx);
    }
}

impl Handler<ProcessMessage> for CsvReader {
    type Result = ();

    fn handle(&mut self, _msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        // CsvReader doesn't handle incoming ProcessMessages
    }
}

impl CsvReader {
    /// Reads data from the CSV file and sends it to the Coordinator.
    fn read_and_send_data(&mut self, ctx: &mut Context<Self>) {
        // Open the CSV file for reading
        let file = File::open(&self.file_path).expect("Failed to open CSV file");
        let reader = BufReader::new(file);

        // Read each line from the CSV
        for line in reader.lines() {
            let start_time = std::time::Instant::now();

            if let Ok(line) = line {
                // Parse the CSV line into a vector of f64 values
                let feature_vector: Vec<f64> = line
                    .split(',')
                    .filter_map(|s| s.trim().parse::<f64>().ok())
                    .collect();

                if feature_vector.is_empty() {
                    continue; // Skip empty lines or invalid rows
                }

                let id = self.next_id;
                self.next_id += 1;
                self.current_count += 1;

                // Determine batch info
                let (batch_id, batch_total) = if self.send_batch_info {
                    if let Some(size) = self.batch_size {
                        if self.current_batch_count >= size {
                            self.current_batch_id += 1;
                            self.current_batch_count = 0;
                        }

                        let current_id = self.current_batch_id;
                        let total = size;
                        self.current_batch_count += 1;

                        (Some(current_id), Some(total))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                // Calculate duration and add trace step
                let duration = start_time.elapsed();
                let mut trace: crate::step::Trace = Default::default(); // Start with an empty trace
                let trace_step =
                    crate::step::TraceStep::new(&self.name, duration, self.params.clone());
                trace.add_step(trace_step);

                // Send the feature vector to the Coordinator
                self.coordinator.do_send(ProcessMessage {
                    id,
                    node_id: self.output_name.clone(),
                    data: feature_vector,
                    batch_id,
                    batch_total,
                    trace,
                });

                info!(
                    "CsvReader '{}' processed and sent CSV line with ID {}",
                    self.name, id
                );
            }
        }

        // Stop the actor after processing the file
        ctx.stop();
    }
}
