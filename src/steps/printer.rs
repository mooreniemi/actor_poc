use crate::coordinator::Coordinator;
use crate::messages::ProcessMessage;
use crate::step::{Step, TraceStep};
use actix::prelude::*;
use log::info;
use serde_json::Value;
use std::time::Instant;

/// Printer Actor
///
/// Receives `ProcessMessage` messages and prints them to stdout with a banner.
pub struct Printer {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub params: Value,
}

impl Step for Printer {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        Printer {
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

impl Actor for Printer {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Printer '{}' started.", self.name);
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        info!("Printer '{}' stopped.", self.name);
    }
}

/// Handler for `ProcessMessage` messages.
impl Handler<ProcessMessage> for Printer {
    type Result = ();

    fn handle(&mut self, mut msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let start_time = Instant::now();

        // Create a banner and print the message details
        let banner = format!("=== Printer '{}' Received Message ===", self.name);
        println!("{}", banner);
        println!("ID: {}", msg.id);
        println!("From Node: {}", msg.node_id);
        println!("Processed Data: {:?}", msg.data);
        if let Some(batch_id) = msg.batch_id {
            println!("Batch ID: {}", batch_id);
        }
        if let Some(batch_total) = msg.batch_total {
            println!("Batch Total: {}", batch_total);
        }
        println!("========================================\n");
        // FIXME: we don't report this out because printer doesn't send back to coordinator
        // Add trace step
        let trace_step = TraceStep::new(&self.name, start_time.elapsed(), self.params.clone());
        msg.trace.add_step(trace_step);
        println!(
            "Trace: {}",
            serde_json::to_string_pretty(&msg.trace).unwrap()
        );
        println!("========================================\n");

        // Printer does not send an output
    }
}
