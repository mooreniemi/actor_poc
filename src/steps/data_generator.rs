use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler};
use log::info;
use rand::Rng;
use serde_json::Value;

use crate::{coordinator::Coordinator, messages::ProcessMessage, step::Step};

/// DataGenerator Actor
///
/// Responsible for generating random feature vectors and sending them to the Coordinator.
pub struct DataGenerator {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub interval_secs: u64,
    pub limit: Option<u32>,
    pub current_count: u32,
    pub next_id: u64,
    pub send_batch_info: bool,
    pub batch_size: Option<u32>,
    pub current_batch_id: u64,
    pub current_batch_count: u32,
    pub params: Value,
}

impl Step for DataGenerator {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        let limit = params
            .get("limit")
            .and_then(|v| v.as_u64()) // Extract the value as a u64 if it's a valid number
            .map(|v| v as u32); // Convert to u32

        let batch_mode = params
            .get("batch_mode")
            .and_then(|v| v.as_bool()) // Extract the value as a boolean
            .unwrap_or(false); // Default to `false` if not found

        let batch_size = params
            .get("batch_size")
            .and_then(|v| v.as_u64()) // Extract the value as a u64 if it's a valid number
            .map(|v| v as u32); // Convert to u32

        DataGenerator {
            name,
            output_name,
            coordinator,
            interval_secs: 1, // Default interval
            limit,
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

impl Actor for DataGenerator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!(
            "DataGenerator '{}' started with batch_mode: {} and batch_size: {:?}.",
            self.name, self.send_batch_info, self.batch_size
        );
        self.generate_data(ctx);
    }
}

impl Handler<ProcessMessage> for DataGenerator {
    type Result = ();

    fn handle(&mut self, _msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        // DataGenerator doesn't handle incoming ProcessMessages
    }
}

impl DataGenerator {
    /// Starts the periodic data generation.
    fn generate_data(&mut self, ctx: &mut Context<Self>) {
        ctx.run_interval(
            std::time::Duration::from_secs(self.interval_secs),
            |act, _ctx| {
                let start_time = std::time::Instant::now();

                // Check if limit is reached
                if let Some(limit) = act.limit {
                    if act.current_count >= limit {
                        info!(
                            "DataGenerator '{}' reached the limit of {} iterations. Stopping.",
                            act.name, limit
                        );
                        _ctx.stop();
                        return;
                    }
                }

                // Generate random feature vector
                let feature_vector = generate_random_features(5);
                let id = act.next_id;
                act.next_id += 1;
                act.current_count += 1;

                // Determine batch info
                let (batch_id, batch_total) = if act.send_batch_info {
                    if let Some(size) = act.batch_size {
                        if act.current_batch_count >= size {
                            act.current_batch_id += 1;
                            act.current_batch_count = 0;
                        }

                        let current_id = act.current_batch_id;
                        let total = size;
                        act.current_batch_count += 1;

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
                    crate::step::TraceStep::new(&act.name, duration, act.params.clone());
                trace.add_step(trace_step);

                // Send the generated features to the Coordinator with trace
                act.coordinator.do_send(ProcessMessage {
                    id,
                    node_id: act.output_name.clone(),
                    data: feature_vector.clone(),
                    batch_id,
                    batch_total,
                    trace,
                });

                info!(
                    "DataGenerator '{}' generated features and sent ProcessMessage with ID {}",
                    act.name, id
                );
            },
        );
    }
}

/// Generates a random feature vector of given size.
fn generate_random_features(size: usize) -> Vec<f64> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen_range(0.0..10.0)).collect()
}
