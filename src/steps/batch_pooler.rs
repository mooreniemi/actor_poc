use std::collections::HashMap;

use actix::{Actor, Addr, Context, Handler};
use log::{debug, info};
use serde_json::Value;

use crate::{
    coordinator::Coordinator,
    messages::ProcessMessage,
    step::{Step, TraceStep},
};

/// Pooling Modes for BatchPooler
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolingMode {
    Window { size: usize }, // Pool based on a fixed window size
    BatchId,                // Pool based on batch_id
}

impl Default for PoolingMode {
    fn default() -> Self {
        PoolingMode::Window { size: 10 }
    }
}

/// BatchPooler Actor
///
/// Responsible for pooling ProcessMessage messages into batches based on the configured mode.
pub struct BatchPooler {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub mode: PoolingMode,
    window_buffer: Vec<ProcessMessage>,
    batch_buffers: HashMap<u64, Vec<ProcessMessage>>,
    pub params: Value,
}

impl Step for BatchPooler {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        let mode = if let Some(size) = params
            .get("window_size") // Get the `window_size` field
            .and_then(|s| s.as_u64())
        {
            // Try to convert it to a u64 (or appropriate numeric type)
            PoolingMode::Window {
                size: size as usize,
            } // Convert u64 to usize
        } else {
            PoolingMode::BatchId // Default to `BatchId` if `window_size` is not present or invalid
        };

        BatchPooler {
            name,
            output_name,
            coordinator,
            mode,
            window_buffer: Vec::new(),
            batch_buffers: HashMap::new(),
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

impl Actor for BatchPooler {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!(
            "BatchPooler '{}' started in {:?} mode.",
            self.name, self.mode
        );
    }
}

impl Handler<ProcessMessage> for BatchPooler {
    type Result = ();

    fn handle(&mut self, mut msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let start_time = std::time::Instant::now();
        debug!("BatchPooler received: {:?}", msg);

        match &self.mode {
            PoolingMode::Window { size } => {
                self.window_buffer.push(msg.clone());

                if self.window_buffer.len() >= *size {
                    debug!(
                        "Will flush pool, window {} >= {}",
                        self.window_buffer.len(),
                        size
                    );
                    let batch = self.window_buffer.clone();
                    self.window_buffer.clear();

                    let batched_data: Vec<f64> =
                        batch.iter().flat_map(|m| m.data.clone()).collect();

                    let duration = start_time.elapsed();
                    let trace_step = TraceStep::new(&self.name, duration, self.params().clone());
                    msg.trace.add_step(trace_step);

                    self.coordinator.do_send(ProcessMessage {
                        id: msg.id,
                        node_id: self.output_name.clone(),
                        data: batched_data,
                        batch_id: msg.batch_id,
                        batch_total: msg.batch_total,
                        trace: msg.trace.clone(),
                    });
                } else {
                    debug!(
                        "Will not flush pool, window {} < {}",
                        self.window_buffer.len(),
                        size
                    );
                }
            }
            PoolingMode::BatchId => {
                if let (Some(batch_id), Some(batch_total)) = (msg.batch_id, msg.batch_total) {
                    let buffer = self.batch_buffers.entry(batch_id).or_insert_with(Vec::new);
                    buffer.push(msg.clone());

                    if buffer.len() >= batch_total as usize {
                        debug!("Will flush pool, batch {} >= {}", buffer.len(), batch_total);
                        let batch = buffer.clone();
                        self.batch_buffers.remove(&batch_id);

                        let batched_data: Vec<f64> =
                            batch.iter().flat_map(|m| m.data.clone()).collect();

                        let duration = start_time.elapsed();
                        let trace_step =
                            TraceStep::new(&self.name, duration, self.params().clone());
                        msg.trace.add_step(trace_step);

                        self.coordinator.do_send(ProcessMessage {
                            id: batch_id,
                            node_id: self.output_name.clone(),
                            data: batched_data,
                            batch_id: Some(batch_id),
                            batch_total: Some(batch.len() as u32),
                            trace: msg.trace.clone(),
                        });
                    } else {
                        debug!(
                            "Will not flush pool, batch {} < {}",
                            buffer.len(),
                            batch_total
                        );
                    }
                }
            }
        }
    }
}
