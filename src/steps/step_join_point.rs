use crate::messages::ProcessMessage;
use crate::step::TraceStep;
use crate::{coordinator::Coordinator, step::Step};
use actix::prelude::*;
use log::info;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// StepJoinPointMode Enum
///
/// Represents the mode in which the StepJoinPoint operates.
/// - `AND`: Waits for all expected inputs before aggregating.
/// - `OR`: Produces an output as soon as any one input is received.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum JoinMode {
    AND,
    OR,
}

impl Default for JoinMode {
    fn default() -> Self {
        JoinMode::AND
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum StepJoinPointOutputMode {
    Flatten,
    Nest,
}

impl Default for StepJoinPointOutputMode {
    fn default() -> Self {
        StepJoinPointOutputMode::Flatten
    }
}

pub struct StepJoinPoint {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub expected_nodes: Vec<String>, // Input names (output names of upstream steps)
    pub mode: JoinMode,
    pub output_mode: StepJoinPointOutputMode, // New field for controlling output mode
    pub pending: HashMap<u64, HashMap<String, Vec<f64>>>, // Maps ID to (input_name -> processed data)
    pub completed_ids: HashMap<u64, usize>,               // Tracks remaining branches for each ID
    pub params: Value,
}

impl StepJoinPoint {
    fn parse_join_mode(params: &serde_json::Value) -> JoinMode {
        params
            .get("mode") // Get the mode field
            .and_then(|m| m.as_str()) // Ensure it's a string
            .map(|m| match m {
                "OR" => JoinMode::OR,
                _ => JoinMode::AND, // Default to AND if no match
            })
            .unwrap_or(JoinMode::AND) // Default to AND if "mode" is missing or invalid
    }

    fn parse_output_mode(params: &serde_json::Value) -> StepJoinPointOutputMode {
        params
            .get("output_mode") // Get the output mode field
            .and_then(|m| m.as_str()) // Ensure it's a string
            .map(|m| match m {
                "NEST" => StepJoinPointOutputMode::Nest,
                _ => StepJoinPointOutputMode::Flatten, // Default to Flatten if no match
            })
            .unwrap_or(StepJoinPointOutputMode::Flatten) // Default to Flatten if "output_mode" is missing or invalid
    }
}

impl Step for StepJoinPoint {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        let expected_nodes = params
            .get("expected_nodes") // Get the 'expected_nodes' from params
            .and_then(|n| n.as_array()) // Ensure it's an array
            .map(|arr| {
                arr.iter() // Iterate over the array
                    .filter_map(|v| v.as_str()) // Convert each element to &str if it's a string
                    .map(|s| s.to_string()) // Convert &str to String
                    .collect::<Vec<String>>()
            }) // Collect into a Vec<String>
            .unwrap_or_else(Vec::new); // Default to an empty Vec if none

        let mode = StepJoinPoint::parse_join_mode(&params);
        let output_mode = StepJoinPoint::parse_output_mode(&params);

        StepJoinPoint {
            name,
            output_name,
            coordinator,
            expected_nodes,
            mode,
            output_mode,
            pending: HashMap::new(),
            completed_ids: HashMap::new(),
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

impl Actor for StepJoinPoint {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!(
            "StepJoinPoint '{}' started in {:?} mode.",
            self.name, self.mode
        );
    }
}

impl Handler<ProcessMessage> for StepJoinPoint {
    type Result = ();

    fn handle(&mut self, mut msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let start_time = std::time::Instant::now();

        info!(
            "StepJoinPoint '{}' received processed data with id {} from '{}': {:?}",
            self.name, msg.id, msg.node_id, msg.data
        );

        match self.mode {
            JoinMode::AND => {
                let key = msg.batch_id.unwrap_or(msg.id); // Use batch_id if present, else id
                let entry = self.pending.entry(key).or_insert_with(HashMap::new);
                entry.insert(msg.node_id.clone(), msg.data.clone());

                info!(
                    "StepJoinPoint '{}' current pending data for Key {}: {:?}",
                    self.name, key, entry
                );

                if self
                    .expected_nodes
                    .iter()
                    .all(|node| entry.contains_key(node))
                {
                    info!(
                        "StepJoinPoint '{}' has received all expected nodes ({:?}) for Key {}, (Entry={:?})",
                        self.name, self.expected_nodes, key, entry
                    );

                    let combined_data: Vec<f64> = self
                        .expected_nodes
                        .iter()
                        .flat_map(|node| entry.get(node).unwrap().clone())
                        .collect();

                    info!(
                        "StepJoinPoint '{}' combining data for Key {}: {:?}",
                        self.name, key, combined_data
                    );

                    let duration = start_time.elapsed();
                    let trace_step = TraceStep::new(&self.name, duration, self.params.clone());
                    msg.trace.add_step(trace_step);

                    self.coordinator.do_send(ProcessMessage {
                        id: key,
                        node_id: self.output_name.clone(),
                        data: combined_data,
                        batch_id: msg.batch_id,
                        batch_total: msg.batch_total,
                        trace: msg.trace.clone(),
                    });

                    self.pending.remove(&key);
                } else {
                    info!(
                        "StepJoinPoint '{}' is waiting for more data for Key {}",
                        self.name, key
                    );
                }
            }
            JoinMode::OR => {
                let key = msg.batch_id.unwrap_or(msg.id); // Use batch_id if present, else id

                if let Some(count) = self.completed_ids.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        info!(
                            "StepJoinPoint '{}' decremented remaining branches for Key {}. Remaining: {}",
                            self.name, key, count
                        );
                        if *count == 0 {
                            self.completed_ids.remove(&key);
                            info!(
                                "StepJoinPoint '{}' removed completed Key {} from tracking.",
                                self.name, key
                            );
                        }
                    }
                    info!(
                        "StepJoinPoint '{}' already processed Key {} in OR mode. Skipping.",
                        self.name, key
                    );
                    return;
                }

                info!(
                    "StepJoinPoint '{}' operating in OR mode. Producing output for Key {}.",
                    self.name, key
                );

                let duration = start_time.elapsed();
                let trace_step = TraceStep::new(&self.name, duration, self.params.clone());
                msg.trace.add_step(trace_step);

                self.coordinator.do_send(ProcessMessage {
                    id: key,
                    node_id: self.output_name.clone(),
                    data: msg.data.clone(),
                    batch_id: msg.batch_id,
                    batch_total: msg.batch_total,
                    trace: msg.trace.clone(),
                });

                let remaining = if self.expected_nodes.len() > 1 {
                    self.expected_nodes.len() - 1
                } else {
                    0
                };
                self.completed_ids.insert(key, remaining);
                info!(
                    "StepJoinPoint '{}' initialized remaining branches for Key {}: {}",
                    self.name, key, remaining
                );

                if remaining == 0 {
                    self.completed_ids.remove(&key);
                    info!(
                        "StepJoinPoint '{}' removed completed Key {} from tracking.",
                        self.name, key
                    );
                }
            }
        }
    }
}
