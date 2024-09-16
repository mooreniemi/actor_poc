// src/messages.rs

use crate::{config::Config, step::Trace};
use actix::prelude::*;

use serde::{Deserialize, Serialize};

/// ProcessMessage
///
/// Represents the completion of a processing step within the DAG.
/// Carries processed data along with optional batching information.
#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct ProcessMessage {
    pub id: u64,                  // Unique identifier for the data
    pub node_id: String,          // Identifier of the node that processed or generated the data
    pub data: Vec<f64>,           // Feature vector (raw or processed)
    pub batch_id: Option<u64>,    // Optional batch identifier (if applicable)
    pub batch_total: Option<u32>, // Optional total number of messages in the batch (if applicable)
    pub trace: Trace,             // Trace data for tracking the message through the pipeline
}

/// Message to initialize the Coordinator with the configuration
#[derive(Message)]
#[rtype(result = "()")]
pub struct Initialize {
    pub config: Config,
}
