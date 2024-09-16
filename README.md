# Actor-Based Directed Acyclic Graph (DAG) System in Rust

Can also execute Python via `py_feature_processor.rs`. See the `./configs/csv_to_numpy.json` and example in `scripts/`.

### cli mode

```
cargo run --release -- --config ./configs/test_all.json --timeout 60
```

### http mode

Http mode necessarily must work a bit differently. It must force a "cycle" of some kind to return the output to the client. It does so by sharing a `sender_map` state via the `Coordinator`. When `http_mode` is true, the `Coordinator` special cases some of the validation logic. The http handler actually directly sends to the `Coordinator`, which sends to downstream steps. The downstream steps were altered from the input `config.json` if necessary. (`DataGenerator`s are removed, `BatchPooler`s in the final position are restricted to single outputs.)

```
cargo run --release -- --config ./configs/test_all.json --http --timeout 60
```

If run in http mode, can send requests:

```
curl -X POST http://localhost:8080/process \
  -H "Content-Type: application/json" \
  -d '{
        "features": [1.0, 2.5, 3.0, 4.7, 44.0, 22.3]
      }'
```

### graphviz

If you have `graphviz` on your local machine you can use the `--graph` mode in the CLI. If you give no file name to that flag it will use whatever you send in as `--config` in `/tmp/` with a `.png` extension. (The `config` is what you parse into a graph).

Check out `resources/test_all.png` for an example output.

## python

Getting Python to work with Rust can be a bit rough here.

```
 (myenv) (base) alex@Alexs-MacBook-Air actor_poc % which python
/Users/alex/Code/actor_poc/myenv/bin/python
(myenv) (base) alex@Alexs-MacBook-Air actor_poc % python --version
Python 3.12.6
```

I had to do:

```
export DYLD_LIBRARY_PATH=$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
```

## todo

1. Generalize from `Vec<f64>` for data.
1. Integrate ONNXRuntime (doesn't build easily on aarch, sadly) or at least torch for local ML inference.
1. Much more rigorous testing of steps and better error handling. In http mode it's possible for the `sender_map` to get filled with junk if intermediate steps fail and never forward to the rest of the graph. The `Coordinator` needs to begin to handle an error branch and forward a `ProcessMessage` that can cancel the downstream. (Or something.)