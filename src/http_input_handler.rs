use std::sync::Arc;

use actix::Addr;
use actix_web::{web, HttpResponse};
use dashmap::DashMap;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::{coordinator::Coordinator, messages::ProcessMessage};

pub async fn handle_http_request(
    coordinator: web::Data<Addr<Coordinator>>,
    json_payload: web::Json<Value>,
    sender_map: Option<Arc<DashMap<u64, oneshot::Sender<Vec<f64>>>>>, // Optional sender_map
) -> actix_web::HttpResponse {
    let request_id = rand::random::<u64>(); // Generate a unique ID for this request

    // Extract the feature data from the JSON payload
    let features = json_payload["features"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_f64())
        .collect::<Vec<f64>>();

    if let Some(sender_map) = sender_map {
        // Access the sender_map's data and process it
        let (tx, rx) = tokio::sync::oneshot::channel::<Vec<f64>>();
        sender_map.insert(request_id, tx);

        coordinator.do_send(ProcessMessage {
            id: request_id,
            node_id: "http_input".to_string(),
            data: features,
            batch_id: Some(request_id),
            // in http mode we only have batch of 1
            batch_total: Some(1),
            trace: Default::default(),
        });

        // Wait for the response or return an error if timeout
        match rx.await {
            Ok(response_data) => {
                sender_map.remove(&request_id); // Clean up the map entry
                HttpResponse::Ok()
                    .insert_header(("dag-request-id", request_id.to_string())) // Add the request_id as a header
                    .json(response_data) // Return the JSON response
            }
            Err(_) => {
                sender_map.remove(&request_id); // Clean up on error
                HttpResponse::InternalServerError()
                    .insert_header(("dag-request-id", request_id.to_string())) // Add the request_id as a header on error
                    .body("Failed to process request")
            }
        }
    } else {
        HttpResponse::BadRequest()
            .insert_header(("dag-request-id", request_id.to_string())) // Add the request_id as a header
            .body("No sender_map available to process the request")
    }
}
