use crate::coordinator::Coordinator;
use crate::messages::ProcessMessage;
use crate::step::{Step, TraceStep};
use actix::prelude::*;
use log::{error, info, warn};
use serde_json::Value;
use std::time::Instant;

/// FeatureProcessor Actor
///
/// Responsible for processing feature data. It can perform operations like normalization and encoding.
/// After processing, it sends the processed data to the Coordinator.
pub struct FeatureProcessor {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub params: Value,
}

impl FeatureProcessor {
    fn process_data(&self, data: &[f64]) -> Vec<f64> {
        match self.output_name.as_str() {
            "normalized_data" => {
                let max = data.iter().cloned().fold(f64::NAN, f64::max);
                if max == 0.0 || max.is_nan() {
                    error!("Normalization failed: max value is zero or NaN");
                    return data.to_vec();
                }
                data.iter().map(|x| x / max).collect()
            }
            "encoded_data" => data.iter().map(|x| x * x).collect(),
            _ => {
                warn!(
                    "Unknown FeatureProcessor '{}', passing data through.",
                    self.output_name
                );
                data.to_vec()
            }
        }
    }
}

impl Step for FeatureProcessor {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        FeatureProcessor {
            name,
            output_name,
            coordinator,
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

impl Actor for FeatureProcessor {
    type Context = Context<Self>;
}

impl Handler<ProcessMessage> for FeatureProcessor {
    type Result = ();

    fn handle(&mut self, mut msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let start_time = Instant::now();
        info!(
            "FeatureProcessor '{}' processing features: {:?}",
            self.name, msg.data
        );

        let processed = self.process_data(&msg.data);

        // Update the trace
        let duration = start_time.elapsed();
        let trace_step = TraceStep::new(&self.name, duration, self.params.clone());
        msg.trace.add_step(trace_step);

        self.coordinator.do_send(ProcessMessage {
            id: msg.id,
            node_id: self.output_name.clone(),
            data: processed,
            batch_id: msg.batch_id,
            batch_total: msg.batch_total,
            trace: msg.trace.clone(),
        });

        info!(
            "FeatureProcessor '{}' processed features in {:?}",
            self.output_name, duration
        );
    }
}
