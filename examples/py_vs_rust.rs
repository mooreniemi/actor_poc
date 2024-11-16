use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

fn one_hot_encode_rust(categories: &[&str]) -> Vec<Vec<u8>> {
    let unique_categories: Vec<&str> = {
        let mut cats = categories.to_vec();
        cats.sort_unstable();
        cats.dedup();
        cats
    };

    let mut one_hot = vec![vec![0; unique_categories.len()]; categories.len()];

    for (i, &cat) in categories.iter().enumerate() {
        if let Some(pos) = unique_categories.iter().position(|&x| x == cat) {
            one_hot[i][pos] = 1;
        }
    }

    one_hot
}

fn one_hot_encode_python(module: &PyModule, categories: &[&str]) -> Vec<Vec<u8>> {
    let py_categories: Vec<&str> = categories.iter().cloned().collect();

    let result: Vec<Vec<u8>> = module
        .getattr("one_hot_encode")
        .expect("Function 'one_hot_encode' not found")
        .call1((py_categories,))
        .expect("Failed to call Python function")
        .extract()
        .expect("Failed to extract Python result");

    result
}

fn list_python_dependencies(py: Python) {
    let pkg_resources = py
        .import("pkg_resources")
        .expect("Failed to import pkg_resources");

    let working_set = pkg_resources
        .getattr("working_set")
        .expect("Failed to get working_set");

    println!("Installed Python dependencies:");
    for dist in working_set
        .iter()
        .expect("Failed to iterate over working_set")
    {
        let dist = dist.expect("Failed to get distribution from working_set"); // Unwrap the Result
        let project_name: String = dist
            .getattr("project_name")
            .expect("Failed to get project_name")
            .extract()
            .expect("Failed to extract project_name");
        let version: String = dist
            .getattr("version")
            .expect("Failed to get version")
            .extract()
            .expect("Failed to extract version");
        println!("{}: {}", project_name, version);
    }
}

// I saw:
// (Python 3.12.6, rustc 1.76.0 (07dca489a 2024-02-04))
// -- DEBUG:
// Rust average duration (10000 iterations): 6.193µs
// Python average duration (10000 iterations): 10.496µs
// -- RELEASE:
// Rust average duration (10000 iterations): 213ns
// Python average duration (10000 iterations): 4.922µs
fn main() {
    // FIXME: this override is not being respected in all cases
    // NOTE: using homebrew python because conda-forge python breaks pyo3 version parsing
    env::set_var("PYTHON_SYS_EXECUTABLE", "/opt/homebrew/bin/python3.12");

    // Set up Python environment variables
    env::set_var(
        "PYTHONHOME",
        "/opt/homebrew/Cellar/python@3.12/3.12.6/Frameworks/Python.framework/Versions/3.12",
    );
    env::set_var(
        "PYTHONPATH",
        "/opt/homebrew/Cellar/python@3.12/3.12.6/Frameworks/Python.framework/Versions/3.12/lib/python3.12/site-packages",
    );

    println!("PYTHON_SYS_EXECUTABLE: {:?}", env::var("PYTHON_SYS_EXECUTABLE"));
    println!("PYTHONHOME: {:?}", env::var("PYTHONHOME"));
    println!("PYTHONPATH: {:?}", env::var("PYTHONPATH"));

    let categories = vec!["cat", "dog", "mouse", "cat", "dog", "elephant"];

    // Get the path to the Python script
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let script_path = Path::new(&manifest_dir).join("scripts/one_hot.py");

    if !script_path.exists() {
        panic!("Python script not found at {:?}", script_path);
    }

    // Read and compile the Python script into a module
    let gil = Python::acquire_gil();
    let py = gil.python();
    println!("Python version: {}", py.version());

    let script = fs::read_to_string(&script_path).expect("Failed to read Python script");
    let module = PyModule::from_code(py, &script, script_path.to_str().unwrap(), "one_hot")
        .expect("Failed to compile Python script");

    // List installed Python dependencies
    list_python_dependencies(py);

    // Benchmark Rust implementation over 10,000 iterations
    let iterations = 10_000;
    let mut rust_total_duration = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        one_hot_encode_rust(&categories);
        rust_total_duration += start.elapsed();
    }
    let rust_avg_duration = rust_total_duration / iterations as u32;
    println!("Rust average duration ({} iterations): {:?}", iterations, rust_avg_duration);

    // Benchmark Python implementation over 10,000 iterations
    let mut python_total_duration = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        one_hot_encode_python(&module, &categories);
        python_total_duration += start.elapsed();
    }
    let python_avg_duration = python_total_duration / iterations as u32;
    println!(
        "Python average duration ({} iterations): {:?}",
        iterations, python_avg_duration
    );
}