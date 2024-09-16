use crate::coordinator::Coordinator;
use crate::messages::ProcessMessage;
use crate::step::Step;
use crate::step::TraceStep;
use actix::prelude::*;
use log::{error, info};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value;
use std::env;
use std::fs;
use std::time::Instant;

/// The idea of this processor is an escape-hatch to let you just play in Python.
/// Obviously it would not be very performant in Production settings.
pub struct PyFeatureProcessor {
    pub name: String,
    pub output_name: String,
    pub coordinator: Addr<Coordinator>,
    pub python_home: Option<String>,   // Path to Python environment
    pub python_path: Option<String>,   // Path to Python packages
    pub python_script: Option<String>, // Path to the Python script
    pub params: Value,                 // Parameters used in the processor
}

impl PyFeatureProcessor {
    /// Initialize the Python environment before running any Python code
    fn initialize_python_env(&self) {
        if let Some(ref home) = self.python_home {
            env::set_var("PYTHONHOME", home);
        }
        if let Some(ref path) = self.python_path {
            env::set_var("PYTHONPATH", path);
        }
        info!(
            "Python environment initialized with PYTHONHOME={:?} and PYTHONPATH={:?}",
            self.python_home, self.python_path
        );
    }

    /// Load and run the Python script, passing the input data
    fn run_python_script(&self, data: Vec<f64>) -> Option<Vec<f64>> {
        if let Some(ref script_path) = self.python_script {
            // Read the Python script from the specified path
            let script_content = fs::read_to_string(script_path);
            match script_content {
                Ok(script) => {
                    // Execute the Python code
                    Python::with_gil(|py| {
                        let locals = PyDict::new(py);
                        locals.set_item("input_data", data.clone()).unwrap();

                        let result = py.run(&script, None, Some(locals));
                        match result {
                            Ok(_) => {
                                let output: Vec<f64> = locals
                                    .get_item("output_data")
                                    .unwrap()
                                    .extract::<Vec<f64>>()
                                    .unwrap();
                                // Extract captured stdout
                                let captured_output: String = locals
                                    .get_item("captured_output")
                                    .unwrap()
                                    .extract::<String>()
                                    .unwrap_or("None".to_owned());
                                info!("Captured Python output was: {}", captured_output);
                                Some(output)
                            }
                            Err(e) => {
                                error!("Python script execution failed: {:?}", e);
                                None
                            }
                        }
                    })
                }
                Err(e) => {
                    error!("Failed to read Python script at '{}': {:?}", script_path, e);
                    None
                }
            }
        } else {
            error!("No Python script path provided in configuration.");
            None
        }
    }
}

impl Step for PyFeatureProcessor {
    fn new_from_params(
        name: String,
        output_name: String,
        coordinator: Addr<Coordinator>,
        params: Value,
    ) -> Self {
        // Extract paths from params and map to String
        let python_home = params
            .get("python_home")
            .and_then(|v| v.as_str()) // Extract string slice
            .map(|s| s.to_string()); // Convert to String

        let python_path = params
            .get("python_path")
            .and_then(|v| v.as_str()) // Extract string slice
            .map(|s| s.to_string()); // Convert to String

        let python_script = params
            .get("python_script")
            .and_then(|v| v.as_str()) // Extract string slice
            .map(|s| s.to_string()); // Convert to String

        PyFeatureProcessor {
            name,
            output_name,
            coordinator,
            python_home,
            python_path,
            python_script,
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

impl Actor for PyFeatureProcessor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("PyFeatureProcessor '{}' started.", self.name);
        self.initialize_python_env(); // Initialize Python environment when the actor starts
    }
}

impl Handler<ProcessMessage> for PyFeatureProcessor {
    type Result = ();

    fn handle(&mut self, mut msg: ProcessMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let start_time = Instant::now();
        info!(
            "PyFeatureProcessor '{}' received data: {:?}",
            self.name, msg.data
        );

        // Run the Python script to process the input data
        let processed = self.run_python_script(msg.data.clone());

        match processed {
            Some(output) => {
                // Update the trace
                let duration = start_time.elapsed();
                let trace_step = TraceStep::new(&self.name, duration, self.params.clone());
                msg.trace.add_step(trace_step);

                self.coordinator.do_send(ProcessMessage {
                    id: msg.id,
                    node_id: self.output_name.clone(),
                    data: output,
                    batch_id: msg.batch_id,
                    batch_total: msg.batch_total,
                    trace: msg.trace.clone(),
                });
                info!(
                    "PyFeatureProcessor '{}' processed data in {:?}",
                    self.output_name,
                    start_time.elapsed()
                );
            }
            None => {
                error!("PyFeatureProcessor '{}' failed to process data.", self.name);
            }
        }
    }
}
