use ort::{
    environment::Environment,
    execution_providers::{ExecutionProvider, CPUExecutionProviderOptions},
    session::SessionBuilder,
    GraphOptimizationLevel, Value,
};
use ndarray::{Array, CowArray};
use std::{path::Path, sync::Arc};
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use CARGO_MANIFEST_DIR to determine the model path dynamically
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let model_path = Path::new(manifest_dir).join("data").join("decision_tree_with_pipeline.onnx");

    // Ensure the model path is valid
    if !model_path.exists() {
        eprintln!("ONNX model not found at {:?}", model_path);
        return Err("ONNX model file missing".into());
    }

    println!("Loading model from {:?}", model_path);

    // Create a single ONNX Runtime environment for the application
    let environment = Arc::new(
        Environment::builder()
            .with_name("InferenceServer")
            .with_execution_providers([ExecutionProvider::CPU(CPUExecutionProviderOptions::default())])
            .build()?,
    );

    // Create a session builder for the model
    let session = Arc::new(
        SessionBuilder::new(&environment)?
            .with_optimization_level(GraphOptimizationLevel::Level1)?
            .with_model_from_file(model_path)?,
    );

    // Start threads for parallel inference
    let mut handles = vec![];
    for i in 0..4 {
        let session = Arc::clone(&session);
        handles.push(thread::spawn(move || {
            let input_data = vec![1.0_f32, 2.0, 3.0, 4.0]; // Input data as f32
            let input_tensor = Array::from_shape_vec((1, input_data.len()), input_data)
                .expect("Failed to create input tensor");

            match perform_inference(&session, input_tensor) {
                Ok(output) => println!("Thread {}: Output: {:?}", i, output),
                Err(e) => eprintln!("Thread {}: Inference failed: {:?}", i, e),
            }
        }));
    }

    // Wait for all threads to finish
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    Ok(())
}

fn perform_inference(
    session: &ort::Session,
    input_tensor: Array<f32, ndarray::Ix2>, // Input tensor as f32
) -> Result<Vec<i64>, Box<dyn std::error::Error>> { // Output as i64
    // Convert to CowArray and then to dynamic dimensions
    let cow_tensor: CowArray<f32, _> = input_tensor.into();
    let dyn_tensor = cow_tensor.into_dyn();

    // Create the input Value and run inference
    let input_value = Value::from_array(session.allocator(), &dyn_tensor)?;
    let outputs = session.run(vec![input_value])?;

    // Extract the output tensor
    if let Some(output) = outputs.get(0) {
        let output_tensor: ort::tensor::OrtOwnedTensor<i64, _> = output.try_extract()?; // Extract as i64
        Ok(output_tensor.view().as_slice().unwrap().to_vec())
    } else {
        Err("No output tensor produced".into())
    }
}