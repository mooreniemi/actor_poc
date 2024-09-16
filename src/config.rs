// src/config.rs

use serde::Deserialize;
use serde_json::Value;

/// Represents the entire DAG configuration, consisting of multiple steps.
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub steps: Vec<StepConfig>,
    // This is set by the way we run the graph
    #[serde(skip_deserializing)] // This will skip deserializing the field
    pub http_mode: bool, // Add this to represent whether we're in HTTP mode
}

/// Represents a single step in the DAG.
#[derive(Deserialize, Debug, Clone)]
pub struct StepConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: String, // "FeatureProcessor", "MLModel", or "DataGenerator"
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    #[serde(default)]
    pub params: Value,
}
