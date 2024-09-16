use log::debug;
use petgraph::dot::{Config as GraphConfig, Dot};
use petgraph::graph::DiGraph;
use serde_json::Value;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::process::Command;

/// A node in the graph representation
#[derive(Debug)]
pub struct StepNode {
    pub name: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

/// Converts the JSON config into a list of `StepNode` structs
pub fn convert_to_step_nodes(config_json: &Value) -> Vec<StepNode> {
    let binding = vec![];
    let steps = config_json["steps"].as_array().unwrap_or(&binding);
    let mut step_nodes = Vec::new();

    for step in steps {
        let name = step["name"].as_str().unwrap_or_default().to_string();
        let inputs = step["inputs"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        let outputs = step["outputs"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();

        step_nodes.push(StepNode {
            name,
            inputs,
            outputs,
        });
    }

    step_nodes
}

/// Creates a directed graph (DAG) from the list of `StepNode` structs
pub fn create_graph(steps: Vec<StepNode>) -> DiGraph<String, ()> {
    let mut graph = DiGraph::new();
    let mut node_map = std::collections::HashMap::new();

    // Add all nodes to the graph
    for step in &steps {
        let node_idx = graph.add_node(step.name.clone());
        node_map.insert(step.name.clone(), node_idx);
    }

    // Add edges based on matching outputs and inputs
    for step in &steps {
        let from_node = &step.name;
        if let Some(&from_idx) = node_map.get(from_node) {
            for output in &step.outputs {
                // Find the steps that use this output as input
                for target_step in &steps {
                    if target_step.inputs.contains(output) {
                        if let Some(&to_idx) = node_map.get(&target_step.name) {
                            // Add an edge from the current step's output to the target step's input
                            graph.add_edge(from_idx, to_idx, ());
                        }
                    }
                }
            }
        }
    }

    graph
}

/// Exports the generated graph to an image (PNG format) using the `dot` command
pub fn export_graph_to_image(
    graph: DiGraph<String, ()>,
    output_path: &str,
) -> Result<(), Box<dyn Error>> {
    // Create a DOT representation of the graph
    let dot_graph = Dot::with_config(&graph, &[GraphConfig::EdgeNoLabel]);

    // Save the DOT representation to a temporary file
    // FIXME: should probably remove this or change name
    let dot_file_path = format!("{}.dot", output_path);
    debug!("Temp file in: {}", dot_file_path);
    let mut dot_file = File::create(&dot_file_path)?;
    use std::io::Write;
    writeln!(dot_file, "{:?}", dot_graph)?;

    // Convert the DOT file to a PNG image using the `dot` command from Graphviz
    let output_image_path = format!("{}.png", output_path);
    debug!("Output image file in: {}", output_image_path);
    let dot_process = Command::new("dot")
        .arg("-Tpng")
        .arg(&dot_file_path)
        .arg("-o")
        .arg(&output_image_path)
        .output()?;

    if !dot_process.status.success() {
        eprintln!(
            "Failed to generate PNG from DOT file. Error: {:?}",
            String::from_utf8_lossy(&dot_process.stderr)
        );
        return Err("Failed to run dot command".into());
    }

    // Clean up the temporary DOT file
    let dot_path = Path::new(&dot_file_path);
    if dot_path.exists() {
        std::fs::remove_file(dot_path)?;
    }

    println!("Graph exported to {}", output_image_path);
    Ok(())
}

/// Read and parse the JSON config file.
pub fn read_config(config_file: &str) -> Result<Value, Box<dyn Error>> {
    let config_content = std::fs::read_to_string(config_file)?;
    let config_json: Value = serde_json::from_str(&config_content)?;
    Ok(config_json)
}
