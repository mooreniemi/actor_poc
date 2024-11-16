use crate::coordinator::Coordinator;
use crate::messages::ProcessMessage;
use crate::step::{Step, TraceStep};
use actix::prelude::*;
use log::{debug, error, info};
use ort::{Environment, GraphOptimizationLevel,  SessionBuilder, Value as OrtValue};
use ndarray::{Array, CowArray};
use reqwest::Client;
use serde_json::{json, Value as JsonValue};
use std::time::Instant;
use std::sync::Arc;

/// MLModel Actor
///
/// Responsible for making predictions based on processed feature data.
/// After making a prediction, it sends the result to the Coordinator.
pub struct MLModel {
    pub name: String,
    pub output_name: String,
    pub params: JsonValue,
    pub coordinator: Addr<Coordinator>,
    pub remote_endpoint: Option<String>,
    pub onnx_model_path: Option<String>,
}

impl MLModel {
    fn handle_local_prediction(&self, msg: ProcessMessage) -> Vec<f64> {
        if let Some(model_path) = &self.onnx_model_path {
            // Create environment wrapped in Arc
            let environment = match Environment::builder()
                .with_name("MLModelEnvironment")
                .build()
            {
                Ok(env) => Arc::new(env),
                Err(e) => {
                    error!("Failed to build environment: {:?}", e);
                    return vec![];
                }
            };

            let session = match SessionBuilder::new(&environment)
                .expect("Failed to create session builder")
                .with_optimization_level(GraphOptimizationLevel::Level1)
                .expect("Failed to set optimization level")
                .with_model_from_file(model_path)
            {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create session: {:?}", e);
                    return vec![];
                }
            };

            // Create input tensor
            let array = Array::from_shape_vec(
                (1, msg.data.len()),
                msg.data.iter().map(|&x| x as f32).collect(),
            ).expect("Failed to create input array");

            let cow_array: CowArray<f32, _> = array.into_dyn().into();

            let input_tensor = OrtValue::from_array(session.allocator(), &cow_array)
                .expect("Failed to create input tensor");

            // Run inference and process results
            let outputs = match session.run(vec![input_tensor]) {
                Ok(o) => o,
                Err(e) => {
                    error!("ONNX inference failed: {:?}", e);
                    return vec![];
                }
            };

            // Process outputs
            match outputs.get(0) {
                Some(output) => {
                    match output.try_extract::<f32>() {
                        Ok(tensor) => tensor.view().iter().map(|&x| x as f64).collect(),
                        Err(e) => {
                            error!("Failed to extract tensor data: {:?}", e);
                            vec![]
                        }
                    }
                }
                None => {
                    error!("ONNX model produced no output tensor");
                    vec![]
                }
            }
        } else {
            // Fallback logic
            match self.output_name.as_str() {
                "lr_output" => vec![msg.data.iter().sum()],
                "am_output" => vec![msg.data.iter().product()],
                _ => vec![msg.data.iter().sum()],
            }
        }
    }
}

impl Step for MLModel {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: JsonValue,
    ) -> Self {
        let remote_endpoint = params
            .get("remote_endpoint")
            .and_then(|ep| ep.as_str())
            .map(|ep| ep.to_string());

        let onnx_model_path = params
            .get("onnx_model_path")
            .and_then(|path| path.as_str())
            .map(|path| path.to_string());

        MLModel {
            name,
            output_name,
            coordinator,
            remote_endpoint,
            params,
            onnx_model_path,
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

    fn params(&self) -> &JsonValue {
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
            let coordinator = self.coordinator.clone();
            let remote_endpoint = remote_endpoint.clone();
            let output_name = self.output_name.clone();
            let params = self.params.clone();

            ctx.spawn(
                async move {
                    let client = Client::new();
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
                                                .filter_map(|v| v.as_f64())
                                                .collect()
                                        })
                                        .unwrap_or_else(|| {
                                            error!("Invalid 'processed_features' in response");
                                            vec![]
                                        })
                                }
                                Err(_) => {
                                    error!("Failed to parse JSON response from remote");
                                    vec![]
                                }
                            };
                            info!("MLModel '{}' processed remotely: {:?}", msg.node_id, result);

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
                            error!("Remote endpoint error: {:?}", response.status());
                        }
                        Err(e) => {
                            error!("Request to remote endpoint failed: {:?}", e);
                        }
                    }
                }
                .into_actor(self),
            );
        } else {
            // Local processing
            let prediction = self.handle_local_prediction(msg.clone());

            info!(
                "MLModel '{}' prediction: {:?} (processed locally)",
                self.output_name, prediction
            );

            let duration = start_time.elapsed();
            let trace_step = TraceStep::new(&self.output_name, duration, self.params.clone());
            msg.trace.add_step(trace_step);

            self.coordinator.do_send(ProcessMessage {
                id: msg.id,
                node_id: self.output_name.clone(),
                data: prediction,
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