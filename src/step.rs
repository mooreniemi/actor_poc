use crate::coordinator::Coordinator;
use crate::messages::ProcessMessage;
use actix::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// Step Trait
///
/// All pipeline steps (actors) will implement this trait, allowing for uniform handling of the name, output name, coordinator address, and params.
pub trait Step: Actor + Handler<ProcessMessage> {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self
    where
        Self: Sized;

    fn name(&self) -> &str;

    fn output_name(&self) -> &str;

    fn coordinator(&self) -> Addr<Coordinator>;

    fn params(&self) -> &Value;
}

/// TraceStep Struct
///
/// This structure captures a step in the pipeline, logging the step name, duration, and relevant parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceStep {
    pub node_id: String,    // Name of the step (node)
    pub duration: Duration, // Duration it took to process the step
    pub params: Value,      // Parameters used in the step
}

impl TraceStep {
    /// Creates a new TraceStep with the given node ID, duration, and params.
    pub fn new(node_id: &str, duration: Duration, params: Value) -> Self {
        TraceStep {
            node_id: node_id.to_string(),
            duration,
            params,
        }
    }
}

/// Trace Struct
///
/// This structure aggregates a list of TraceStep objects, representing the full trace of a message across the pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Trace {
    pub steps: Vec<TraceStep>, // All steps of the trace
}

impl Trace {
    /// Adds a new step to the trace.
    pub fn add_step(&mut self, step: TraceStep) {
        self.steps.push(step);
    }

    /// Combines another trace with this one.
    pub fn merge(&mut self, other: Trace) {
        self.steps.extend(other.steps);
    }
}
