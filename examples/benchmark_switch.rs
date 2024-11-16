use std::time::Instant;

// Define a trait with a simple operation
trait Operation {
    fn execute(&self, input: i32) -> i32;
}

// Implement the trait for a specific type
struct AddOperation;
impl Operation for AddOperation {
    fn execute(&self, input: i32) -> i32 {
        input + 1
    }
}

// Benchmark using dynamic dispatch (dyn)
fn benchmark_dynamic_dispatch(op: &dyn Operation, iterations: i32) -> i32 {
    let mut result = 0;
    for _ in 0..iterations {
        result = op.execute(result);
    }
    result
}

// Benchmark using static dispatch (generics)
fn benchmark_static_dispatch<T: Operation>(op: &T, iterations: i32) -> i32 {
    let mut result = 0;
    for _ in 0..iterations {
        result = op.execute(result);
    }
    result
}

// Helper function to run trials
fn run_benchmark(iterations: i32) {
    let op = AddOperation;

    println!("\nRunning benchmark with {} iterations:", iterations);

    // Benchmark dynamic dispatch
    let start = Instant::now();
    let result_dynamic = benchmark_dynamic_dispatch(&op, iterations);
    let duration_dynamic = start.elapsed();
    println!(
        "Dynamic Dispatch Result: {}, Time: {:?}",
        result_dynamic, duration_dynamic
    );

    // Benchmark static dispatch
    let start = Instant::now();
    let result_static = benchmark_static_dispatch(&op, iterations);
    let duration_static = start.elapsed();
    println!(
        "Static Dispatch Result: {}, Time: {:?}",
        result_static, duration_static
    );

    println!(
        "Delta (Dynamic - Static): {:?}",
        duration_dynamic.checked_sub(duration_static)
    );
}

/*
An example run:
-------------------------------------------------
Running `target/release/examples/benchmark_switch`

Running benchmark with 100000 iterations:
Dynamic Dispatch Result: 100000, Time: 83ns
Static Dispatch Result: 100000, Time: 42ns
Delta (Dynamic - Static): Some(41ns)

Running benchmark with 1000000 iterations:
Dynamic Dispatch Result: 1000000, Time: 41ns
Static Dispatch Result: 1000000, Time: 41ns
Delta (Dynamic - Static): Some(0ns)

Running benchmark with 10000000 iterations:
Dynamic Dispatch Result: 10000000, Time: 42ns
Static Dispatch Result: 10000000, Time: 42ns
Delta (Dynamic - Static): Some(0ns)

Running benchmark with 100000000 iterations:
Dynamic Dispatch Result: 100000000, Time: 42ns
Static Dispatch Result: 100000000, Time: 42ns
Delta (Dynamic - Static): Some(0ns)
*/
fn main() {
    // Run the benchmarks with different iteration counts
    let trials = vec![100_000, 1_000_000, 10_000_000, 100_000_000];
    for &iterations in &trials {
        run_benchmark(iterations);
    }
}
