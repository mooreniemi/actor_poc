use crate::config::{Config, StepConfig};
use crate::messages::{Initialize, ProcessMessage};
use crate::step::Step;
use crate::steps::batch_pooler::BatchPooler;
use crate::steps::csv_reader::CsvReader;
use crate::steps::data_generator::DataGenerator;
use crate::steps::feature_processor::FeatureProcessor;
use crate::steps::http_output::HttpOutput;
use crate::steps::ml_model::MLModel;
use crate::steps::printer::Printer;
use crate::steps::py_feature_processor::PyFeatureProcessor;
use crate::steps::step_join_point::StepJoinPoint;
use actix::prelude::*;
use dashmap::DashMap;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::oneshot;

/// Enum to encapsulate different step actors
enum StepActor {
    FeatureProcessor(Addr<FeatureProcessor>),
    PyFeatureProcessor(Addr<PyFeatureProcessor>),
    MLModel(Addr<MLModel>),
    DataGenerator(Addr<DataGenerator>),
    CsvReader(Addr<CsvReader>),
    StepJoinPoint(Addr<StepJoinPoint>),
    BatchPooler(Addr<BatchPooler>),
    Printer(Addr<Printer>),
    HttpOutput(Addr<HttpOutput>),
}

impl StepActor {
    fn send_process_message(&self, msg: ProcessMessage) {
        match self {
            StepActor::FeatureProcessor(actor) => actor.do_send(msg),
            StepActor::PyFeatureProcessor(actor) => actor.do_send(msg),
            StepActor::MLModel(actor) => actor.do_send(msg),
            StepActor::DataGenerator(actor) => actor.do_send(msg),
            StepActor::CsvReader(actor) => actor.do_send(msg),
            StepActor::StepJoinPoint(actor) => actor.do_send(msg),
            StepActor::BatchPooler(actor) => actor.do_send(msg),
            StepActor::Printer(actor) => actor.do_send(msg),
            StepActor::HttpOutput(actor) => actor.do_send(msg),
        }
    }
}

/// Coordinator Actor
///
/// Manages the Directed Acyclic Graph (DAG) by routing data between actors based on the configuration.
/// It spawns actors, registers their addresses, and handles the flow of processed data.
pub struct Coordinator {
    // Mapping from step name to its StepActor
    actors: HashMap<String, StepActor>,
    // Mapping from input name to its downstream step names
    adjacency: HashMap<String, Vec<String>>,
    sender_map: Option<Arc<DashMap<u64, oneshot::Sender<Vec<f64>>>>>, // Optional sender_map
}

impl Coordinator {
    /// Creates a new Coordinator with the given configuration.
    pub fn new(
        config: Config,
        sender_map: Option<Arc<DashMap<u64, oneshot::Sender<Vec<f64>>>>>,
    ) -> Self {
        // Build adjacency list based on step inputs to downstream steps
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
        for step in &config.steps {
            for input in &step.inputs {
                adjacency
                    .entry(input.clone())
                    .or_insert_with(Vec::new)
                    .push(step.name.clone());
            }
        }

        Coordinator {
            actors: HashMap::new(),
            adjacency,
            sender_map,
        }
    }

    fn create_step_actor(&self, step: &StepConfig, ctx: &mut Context<Self>) -> Option<StepActor> {
        match step.node_type.as_str() {
            "FeatureProcessor" => {
                let output_name = step.outputs.get(0)?;
                let processor = FeatureProcessor::new_from_params(
                    step.name.clone(),
                    output_name.to_string(),
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::FeatureProcessor(processor.start()))
            }
            "PyFeatureProcessor" => {
                let output_name = step.outputs.get(0)?;
                let processor = PyFeatureProcessor::new_from_params(
                    step.name.clone(),
                    output_name.to_string(),
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::PyFeatureProcessor(processor.start()))
            }
            "MLModel" => {
                let output_name = step.outputs.get(0)?;
                let model = MLModel::new_from_params(
                    step.name.clone(),
                    output_name.to_string(),
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::MLModel(model.start()))
            }
            "DataGenerator" => {
                let output_name = step.outputs.get(0)?;
                let data_generator = DataGenerator::new_from_params(
                    step.name.clone(),
                    output_name.to_string(),
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::DataGenerator(data_generator.start()))
            }
            "CsvReader" => {
                let csv_reader = CsvReader::new_from_params(
                    step.name.clone(),
                    step.outputs[0].clone(),
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::CsvReader(csv_reader.start()))
            }
            "StepJoinPoint" => {
                let output_name = step.outputs.get(0)?;
                // FIXME: this might be a bit confusing but we're grabbing the inputs and turning them into params
                // this makes it easier to track what we needed, we might want to do this everywhere
                // Inject `inputs` as `expected_nodes` into params
                // Ensure that the params is a serde_json::Map
                let mut params = step.params.clone().as_object().cloned().unwrap_or_default();

                // Inject `inputs` as `expected_nodes` into params
                params.insert(
                    "expected_nodes".to_string(),
                    serde_json::Value::Array(
                        step.inputs
                            .iter()
                            .map(|input| serde_json::Value::String(input.clone()))
                            .collect(),
                    ),
                );
                let step_join_point = StepJoinPoint::new_from_params(
                    step.name.clone(),
                    output_name.to_string(),
                    ctx.address(),
                    serde_json::Value::Object(params),
                );
                Some(StepActor::StepJoinPoint(step_join_point.start()))
            }
            "BatchPooler" => {
                let output_name = step.outputs.get(0)?;
                let pooler = BatchPooler::new_from_params(
                    step.name.clone(),
                    output_name.to_string(),
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::BatchPooler(pooler.start()))
            }
            "Printer" => {
                let printer = Printer::new_from_params(
                    step.name.clone(),
                    "".to_string(), // No output name for Printer
                    ctx.address(),
                    step.params.clone(),
                );
                Some(StepActor::Printer(printer.start()))
            }
            "HttpOutput" => {
                let http_output = HttpOutput::new(
                    step.name.clone(),
                    step.inputs.get(0).unwrap_or(&"".to_string()).clone(), // Handle inputs
                    self.sender_map.clone()?,
                );
                Some(StepActor::HttpOutput(http_output.start()))
            }
            _ => {
                warn!("Unknown step type: '{}'", step.node_type);
                None
            }
        }
    }

    /// Spawns actors based on the configuration and registers their addresses.
    fn spawn_actors(&mut self, ctx: &mut Context<Self>, config: &Config) {
        for step in &config.steps {
            if let Some(actor) = self.create_step_actor(step, ctx) {
                self.actors.insert(step.name.clone(), actor);
                info!("Spawned actor for step '{}'", step.name);
            }
        }
    }

    /// Validates the configuration for consistency and correctness.
    fn validate_config(&self, config: &Config) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // 1. Batch Pooler and Data Generator Consistency
        for step in &config.steps {
            if step.node_type == "BatchPooler" {
                let mode = step
                    .params
                    .get("mode")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Window");

                if mode == "BatchId" {
                    let has_batch_generator = config.steps.iter().any(|s| {
                        s.node_type == "DataGenerator"
                            && s.params
                                .get("batch_mode")
                                .and_then(|bm| bm.as_bool()) // Fix the incorrect `parse` method by using `as_bool()`
                                .unwrap_or(false)
                    });

                    if !has_batch_generator {
                        errors.push(format!(
                            "BatchPooler '{}' is set to 'BatchId' mode, but no DataGenerator is configured with 'batch_mode: true'.",
                            step.name
                        ));
                    }
                }
            }
        }

        // 2. DAG Structural Integrity
        let sources: Vec<&StepConfig> = config
            .steps
            .iter()
            .filter(|s| s.inputs.is_empty())
            .collect();
        let sinks: Vec<&StepConfig> = config
            .steps
            .iter()
            .filter(|s| s.outputs.is_empty())
            .collect();

        // when http_mode is on the initial "step" is an http handler
        if !config.http_mode && sources.len() != 1 {
            errors.push(format!(
                "DAG must have exactly one source (step with no inputs). Found {}.",
                sources.len()
            ));
        }

        if sinks.len() != 1 {
            errors.push(format!(
                "DAG must have exactly one sink (step with no outputs). Found {}.",
                sinks.len()
            ));
        }

        if self.has_cycles(config) {
            errors.push("The DAG contains cycles. Please ensure it is acyclic.".to_string());
        }

        let all_producers: HashSet<String> = config
            .steps
            .iter()
            .flat_map(|s| s.outputs.iter().cloned())
            .collect();
        let mut steps_with_no_producer = Vec::new();

        // Check if every input has a corresponding producer step
        for step in &config.steps {
            for input in &step.inputs {
                if !all_producers.contains(input) {
                    steps_with_no_producer.push(step.name.clone());
                }
            }
        }

        // Allow only one step to have no producer in http_mode
        if config.http_mode {
            if steps_with_no_producer.len() > 1 {
                errors.push(format!(
            "In http_mode, only one step can have no corresponding producer. Found {} steps without producers: {:?}",
            steps_with_no_producer.len(),
            steps_with_no_producer
        ));
            }
        } else {
            // In non-http_mode, no steps are allowed without a producer
            if !steps_with_no_producer.is_empty() {
                for step_name in steps_with_no_producer {
                    errors.push(format!(
                        "Input for step '{}' does not have a corresponding producer step.",
                        step_name
                    ));
                }
            }
        }

        let mut names = HashSet::new();
        for step in &config.steps {
            if !names.insert(&step.name) {
                errors.push(format!(
                    "Duplicate step name found: '{}'. Step names must be unique.",
                    step.name
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Checks if the DAG has any cycles using DFS.
    fn has_cycles(&self, config: &Config) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
        for step in &config.steps {
            for input in &step.inputs {
                adjacency
                    .entry(input.as_str())
                    .or_insert_with(Vec::new)
                    .push(step.name.as_str());
            }
        }

        for step in &config.steps {
            if self.detect_cycle_util(step.name.as_str(), &adjacency, &mut visited, &mut rec_stack)
            {
                return true;
            }
        }

        false
    }

    /// Utility function for cycle detection.
    fn detect_cycle_util(
        &self,
        node: &str,
        adjacency: &HashMap<&str, Vec<&str>>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        if !visited.contains(node) {
            visited.insert(node.to_string());
            rec_stack.insert(node.to_string());

            if let Some(neighbors) = adjacency.get(node) {
                for &neighbor in neighbors {
                    if !visited.contains(neighbor)
                        && self.detect_cycle_util(neighbor, adjacency, visited, rec_stack)
                    {
                        return true;
                    } else if rec_stack.contains(neighbor) {
                        return true;
                    }
                }
            }
        }
        rec_stack.remove(node);
        false
    }

    /// Handles the `Initialize` message, validates the configuration, and spawns actors.
    fn handle_initialize(&mut self, msg: Initialize, ctx: &mut Context<Self>) {
        match self.validate_config(&msg.config) {
            Ok(()) => {
                info!("Configuration validated successfully.");
                self.spawn_actors(ctx, &msg.config);
            }
            Err(errors) => {
                for error in errors {
                    error!("Configuration error: {}", error);
                }
                System::current().stop();
            }
        }
    }
}

impl Actor for Coordinator {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Coordinator started.");
    }
}

impl Handler<Initialize> for Coordinator {
    type Result = ();

    fn handle(&mut self, msg: Initialize, ctx: &mut Context<Self>) -> Self::Result {
        self.handle_initialize(msg, ctx);
    }
}

impl Handler<ProcessMessage> for Coordinator {
    type Result = ();

    fn handle(&mut self, msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        info!(
            "Coordinator received message from '{}': ID={}, BatchID={:?}",
            msg.node_id, msg.id, msg.batch_id
        );

        // Find downstream steps based on the output name (`msg.node_id`)
        if let Some(downs) = self.adjacency.get(&msg.node_id) {
            for down in downs {
                if let Some(actor) = self.actors.get(down) {
                    actor.send_process_message(msg.clone());
                    info!("Forwarded message to '{}'", down);
                } else {
                    error!("Downstream step '{}' not found", down);
                }
            }
        } else {
            warn!("No downstream steps found for node '{}'", msg.node_id);
        }
    }
}
