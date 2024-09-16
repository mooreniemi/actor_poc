use actix::prelude::*;
use actix_web::{web, App, HttpServer};
use actor_poc::graph_visualizer::{convert_to_step_nodes, create_graph, export_graph_to_image};
use actor_poc::{config::Config, http_input_handler::handle_http_request};
use clap::{Arg, Command};
use env_logger::Env;
use serde_json::{json, Value};
use std::error::Error;
use std::fs;
use std::sync::Arc;

use actor_poc::coordinator::Coordinator;
use actor_poc::messages::Initialize;
use dashmap::DashMap;
use log::{debug, info};
use tokio::sync::oneshot;

#[actix::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // CLI Argument Parsing
    let matches = Command::new("Actor DAG System")
        .version("1.0")
        .about("Runs a DAG system based on actor model using a JSON configuration")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom configuration file in JSON format")
                .required(true)
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("timeout")
                .short('t')
                .long("timeout")
                .value_name("SECONDS")
                .help("Sets an optional timeout duration (in seconds) for the Actix system (default is 30 seconds)")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Sets the level of verbosity to debug")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("graph")
                .long("graph")
                .value_name("OUTPUT_IMAGE")
                .help("[Requires native graphviz lib] Generates a graph image from the configuration file, optionally change prefix (from /tmp/<config_name>")
                .num_args(0..=1) // Allow 0 or 1 arguments
                .value_parser(clap::value_parser!(String)) // Use String instead of Option<String>
        )
        .arg(
            Arg::new("http")
                .long("http")
                .help("Runs the system in HTTP mode, replacing the DataGenerator or CsvReader step with a (virtual) HttpInput step")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Sets the port for the Actix Web server (default is 8080)")
                .value_parser(clap::value_parser!(u16)),
        )
        .get_matches();

    // Initialize the logger based on the verbose flag
    let log_level = if matches.get_flag("verbose") {
        "debug"
    } else {
        "info"
    };
    env_logger::Builder::from_env(Env::default().default_filter_or(log_level)).init();

    // Get the configuration file path and load the JSON
    let config_file = matches
        .get_one::<String>("config")
        .ok_or("Configuration file not provided")?;
    let config_content = fs::read_to_string(config_file)?;

    // Parse the configuration file as JSON
    let mut config_json: Value = serde_json::from_str(&config_content)?;

    if matches.contains_id("graph") {
        // If the graph argument was provided, use it, otherwise create a default path
        let graph_output_path = if let Some(path) = matches.get_one::<String>("graph") {
            Some(path.clone()) // Use Some here to match the type
        } else {
            let config_path = std::path::Path::new(config_file);
            let file_stem = config_path
                .file_stem()
                .unwrap_or_default()
                .to_str()
                .unwrap_or("output");
            Some(format!("/tmp/{}", file_stem)) // Wrap in Some
        };

        if let Some(graph_output_path) = graph_output_path {
            // Convert config_json to Vec<StepNode>
            let config_json: Value = serde_json::from_str(&config_content)?;
            let step_nodes = convert_to_step_nodes(&config_json);

            // Call the function to create the graph
            let graph = create_graph(step_nodes);
            export_graph_to_image(graph, &graph_output_path)?;

            info!("Graph image saved to {}.png", graph_output_path);
            return Ok(());
        }
    }

    // Determine if we are in HTTP mode or CLI mode
    let http_mode = matches.get_flag("http");
    info!("http_mode was: {}", http_mode);

    info!("Starting the Actor-based DAG system.");
    // Modify the JSON config based on HTTP mode
    if http_mode {
        let mut data_generator_output: Option<String> = None;

        // Remove DataGenerator and store its output name
        if let Some(steps) = config_json.get_mut("steps").and_then(|v| v.as_array_mut()) {
            steps.retain(|step| {
                if step["type"] == "DataGenerator" || step["type"] == "CsvReader" {
                    info!("Removing DataGenerator.");
                    // Safely extract the first output as a string
                    if let Some(output) = step["outputs"]
                        .as_array()
                        .and_then(|arr| arr.get(0).and_then(|v| v.as_str()))
                    {
                        data_generator_output = Some(output.to_string());
                    }
                    false // Remove the DataGenerator step
                } else {
                    true // Keep all other steps
                }
            });

            // Update the next step's input from DataGenerator's output to "http_input"
            if let Some(output_name) = data_generator_output {
                for step in steps.iter_mut() {
                    if let Some(inputs) = step["inputs"].as_array_mut() {
                        // Find the output name in the inputs and replace it with "http_input"
                        for input in inputs.iter_mut() {
                            if input == &json!(output_name) {
                                *input = json!("http_input");
                            }
                        }
                    }
                }
            }

            // Find and update the final step
            if let Some(final_step) = steps.iter_mut().rev().find(|step| {
                step["outputs"]
                    .as_array()
                    .map(|o| o.is_empty())
                    .unwrap_or(false)
            }) {
                info!("Replacing final step with HttpOutput.");
                let input_name = final_step["inputs"][0].clone();
                final_step["type"] = json!("HttpOutput");
                final_step["name"] = json!("http_output");
                final_step["outputs"] = json!([]); // HttpOutput has no outputs
                final_step["inputs"] = json!([input_name]);

                // Adjust the BatchPooler preceding the final step
                if let Some(batch_pooler_step) = steps.iter_mut().find(|step| {
                    step["type"] == "BatchPooler"
                        && step["outputs"].as_array().unwrap().contains(&input_name)
                }) {
                    info!(
                        "Adjusting BatchPooler window size to 1 for '{}'",
                        batch_pooler_step["name"].as_str().unwrap()
                    );
                    batch_pooler_step["params"]["window_size"] = json!(1);
                }
            }
        }

        debug!(
            "http mode on, changed config_json to: {}",
            serde_json::to_string_pretty(&config_json).unwrap()
        );
    }

    // Deserialize the modified JSON into the Config struct
    let mut config: Config = serde_json::from_value(config_json)?;
    config.http_mode = http_mode;

    // If http_mode is enabled, create a sender_map, otherwise set it to None
    let sender_map = if http_mode {
        Some(Arc::new(DashMap::<u64, oneshot::Sender<Vec<f64>>>::new()))
    } else {
        None
    };

    // Pass sender_map to the Coordinator when it's created
    let coordinator = Coordinator::new(config.clone(), sender_map.clone()).start();
    coordinator.do_send(Initialize {
        config: config.clone(),
    });

    // If in HTTP mode, start the Actix Web server
    if http_mode {
        let port: u16 = *matches.get_one::<u16>("port").unwrap_or(&8080);

        info!("Starting Actix Web server on port {}", port);

        let server = HttpServer::new(move || {
            let sender_map_clone = sender_map.clone(); // Clone the Option<Arc> here

            App::new()
                .app_data(web::Data::new(coordinator.clone()))
                .route(
                    "/process",
                    web::post().to(move |data, payload| {
                        handle_http_request(data, payload, sender_map_clone.clone())
                        // Clone the Arc inside Option
                    }),
                )
        })
        .bind(("0.0.0.0", port))?
        .run();

        let server_handle = actix::spawn(server);

        let _ = server_handle.await?;
    } else {
        // Get the optional timeout value from the CLI arguments, default to 30 seconds
        let timeout: u64 = *matches.get_one::<u64>("timeout").unwrap_or(&30);
        info!("Using timeout of {} seconds", timeout);

        // If not in HTTP mode, run the system in CLI mode (with a timeout)
        coordinator.do_send(Initialize { config });
        actix::clock::sleep(std::time::Duration::from_secs(timeout)).await;
        System::current().stop();
    }

    Ok(())
}
