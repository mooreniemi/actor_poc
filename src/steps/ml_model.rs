use crate::coordinator::Coordinator;
use crate::messages::ProcessMessage;
use crate::step::{Step, TraceStep};
use actix::prelude::*;
use log::{debug, error, info};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Instant;

/// MLModel Actor
///
/// Responsible for making predictions based on processed feature data.
/// After making a prediction, it sends the result to the Coordinator.
pub struct MLModel {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub remote_endpoint: Option<String>, // Optional remote processing endpoint
    pub params: Value,
}

impl MLModel {
    /// Handle local prediction
    // FIXME: should really use ONNX but that doesn't support aarch (easily at least)
    fn handle_local_prediction(&self, msg: ProcessMessage) -> f64 {
        match self.output_name.as_str() {
            "lr_output" => msg.data.iter().sum::<f64>(),
            "am_output" => msg.data.iter().product::<f64>(),
            _ => msg.data.iter().sum::<f64>(),
        }
    }
}

impl Step for MLModel {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        let remote_endpoint = params
            .get("remote_endpoint")
            .and_then(|ep| ep.as_str())
            .map(|ep| ep.to_string());

        MLModel {
            name,
            output_name,
            coordinator,
            remote_endpoint,
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

impl Actor for MLModel {
    type Context = Context<Self>;
}

impl Handler<ProcessMessage> for MLModel {
    type Result = ();

    fn handle(&mut self, mut msg: ProcessMessage, ctx: &mut Context<Self>) -> Self::Result {
        let start_time = Instant::now();
        if msg.data.is_empty() {
            error!("Received empty feature data in MLModel '{}'", self.name);
            return;
        }

        // Infer processing mode based on the presence of `remote_endpoint`
        if let Some(remote_endpoint) = &self.remote_endpoint {
            // Remote processing
            let coordinator = self.coordinator.clone(); // Move the coordinator for async task
            let remote_endpoint = remote_endpoint.clone(); // Clone endpoint into task
            let output_name = self.output_name.clone();
            let params = self.params.clone();

            ctx.spawn(
                async move {
                    let client = Client::new(); // Create the client only inside the task
                    let input_data = json!({ "features": msg.data });
                    debug!("Sending payload: {:?}", input_data);
                    let response = client.post(&remote_endpoint).json(&input_data).send().await;

                    match response {
                        Ok(response) if response.status().is_success() => {
                            let result: Vec<f64> = match response.json::<serde_json::Value>().await {
                                Ok(json_response) => {
                                    json_response
                                        .get("processed_features")
                                        .and_then(|features| features.as_array())
                                        .map(|array| {
                                            array
                                                .iter()
                                                .filter_map(|v| v.as_f64())  // Extracting valid f64 values
                                                .collect::<Vec<f64>>()
                                        })
                                        .unwrap_or_else(|| {
                                            error!("Invalid or missing 'processed_features' field in response");
                                            vec![]
                                        })
                                }
                                Err(_) => {
                                    error!("Failed to parse JSON response from remote processing");
                                    vec![]
                                }
                            };
                            info!(
                                "MLModel '{}' successfully processed remotely: {:?}",
                                msg.node_id, result
                            );

                            // Update trace with the local processing time and params
                            let duration = start_time.elapsed();
                            let trace_step = TraceStep::new(&output_name, duration, params);
                            msg.trace.add_step(trace_step);

                            coordinator.do_send(ProcessMessage {
                                id: msg.id,
                                node_id: output_name,
                                data: result,
                                batch_id: msg.batch_id,
                                batch_total: msg.batch_total,
                                trace: msg.trace.clone(),
                            });
                        }
                        Ok(response) => {
                            error!(
                                "MLModel '{}' received error from remote endpoint: {:?}",
                                msg.node_id,
                                response.status()
                            );
                        }
                        Err(e) => {
                            error!(
                                "MLModel '{}' failed to send request: {:?}",
                                msg.node_id, e
                            );
                        }
                    }
                }
                .into_actor(self),
            );
        } else {
            // Local processing
            let prediction = self.handle_local_prediction(msg.clone());

            info!(
                "MLModel '{}' prediction: {} (processed locally)",
                self.output_name, prediction
            );

            // Update trace with the local processing time and params
            let duration = start_time.elapsed();
            let trace_step = TraceStep::new(&self.output_name, duration, self.params.clone());
            msg.trace.add_step(trace_step);

            // Send result to Coordinator
            self.coordinator.do_send(ProcessMessage {
                id: msg.id,
                node_id: self.output_name.clone(),
                data: vec![prediction],
                batch_id: msg.batch_id,
                batch_total: msg.batch_total,
                trace: msg.trace.clone(),
            });
        }

        info!(
            "MLModel '{}' handled processing in {:?}",
            self.output_name,
            start_time.elapsed()
        );
    }
}
