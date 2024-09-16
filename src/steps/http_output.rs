use crate::{messages::ProcessMessage, step::Step};
use actix::{Actor, Context, Handler};
use dashmap::DashMap;
use log::{debug, info};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::oneshot;

/// HttpOutput Actor
///
/// Responsible for sending the final data back to the HTTP request via the sender map.
pub struct HttpOutput {
    pub name: String,
    pub input_name: String,
    pub sender_map: Arc<DashMap<u64, oneshot::Sender<Vec<f64>>>>, // Shared map for request senders
}

impl HttpOutput {
    pub fn new(
        name: String,
        input_name: String,
        sender_map: Arc<DashMap<u64, oneshot::Sender<Vec<f64>>>>,
    ) -> Self {
        HttpOutput {
            name,
            input_name,
            sender_map,
        }
    }
}

impl Step for HttpOutput {
    fn new_from_params(
        _name: String,
        _input_name: String,
        _coordinator: actix::Addr<crate::coordinator::Coordinator>,
        _params: Value,
    ) -> Self {
        unimplemented!("The HttpOutput step is special and needs an additional input others don't; the normal method won't work.");
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_name(&self) -> &str {
        &self.input_name
    }

    fn coordinator(&self) -> actix::Addr<crate::coordinator::Coordinator> {
        unimplemented!()
    }

    fn params(&self) -> &Value {
        unimplemented!()
    }
}

impl Actor for HttpOutput {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("HttpOutput '{}' started.", self.name);
    }
}

impl Handler<ProcessMessage> for HttpOutput {
    type Result = ();

    fn handle(&mut self, msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        debug!("HttpOutput: Received {:?}", msg);
        debug!("HttpOutput had access to {:?}", self.sender_map);
        // Look up the sender in the DashMap using the request id
        if let Some((_req_id, sender)) = self.sender_map.remove(&msg.id) {
            // Send the processed data back to the original request
            if sender.send(msg.data.clone()).is_err() {
                info!(
                    "HttpOutput: Failed to send response back to request ID: {}",
                    msg.id
                );
            } else {
                info!(
                    "HttpOutput: Successfully sent response back for request ID: {}",
                    msg.id
                );
            }
        } else {
            info!("HttpOutput: No sender found for request ID: {}", msg.id);
        }
    }
}
