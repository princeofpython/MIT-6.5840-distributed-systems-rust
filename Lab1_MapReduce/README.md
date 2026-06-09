# Lab 1 — MapReduce

Distributed MapReduce implementation in Rust following the [MIT 6.5840 Lab 1 spec](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html).

## Architecture

- **Coordinator** — listens on TCP `127.0.0.1:7777`, assigns map/reduce tasks to workers, tracks task state (`Idle` → `InProgress` → `Done`), runs background timeout checker
- **Worker** — connects to coordinator, requests tasks, executes map/reduce logic, notifies coordinator on completion
- **RPC** — length-prefixed JSON over TCP (`[4 bytes: length][N bytes: JSON body]`)

## Running

**Terminal 1 — Coordinator:**
```sh
cargo run --bin mrcoordinator -- <input_folder> <n_reduce>
```

**Terminal 2+ — Workers:**
```sh
cargo run --bin mrworker -- <worker_id>
```

Example:
```sh
cargo run --bin mrcoordinator -- ../inputs 4
cargo run --bin mrworker -- 1
cargo run --bin mrworker -- 2
```

## Map Phase

- Coordinator reads all input files from `<input_folder>`, creates one `MapTask` per file
- Worker receives a task (file path + `n_reduce`), reads the file, splits into words
- Each word is hashed with FNV-1a and assigned to a bucket: `bucket = fnv1a(word) % n_reduce`
- Output written to `intermediate/mr-{task_id}-{bucket}` (atomic: write to `.tmp` then rename)
- Worker notifies coordinator on completion; coordinator marks task `Done`

## Reduce Phase

- Coordinator waits until all map tasks are `Done`, then assigns reduce tasks
- Each reduce task ID equals a bucket number (`0..n_reduce`)
- Worker globs `intermediate/mr-*-{bucket}` to collect all map outputs for that bucket
- Deserializes all `(word, "1")` pairs, sorts by key, counts consecutive identical keys
- Output written to `output/mr-out-{bucket}` (atomic: write to `.tmp` then rename)
- Worker notifies coordinator on completion; coordinator marks task `Done`

## File Layout

```
intermediate/
├── mr-0-0   mr-0-1   mr-0-2   mr-0-3    ← map task 0, 4 buckets
├── mr-1-0   mr-1-1   mr-1-2   mr-1-3    ← map task 1, 4 buckets
└── ...                                   ← one row per input file

output/
├── mr-out-0    ← reduce task 0 (all bucket-0 pairs across all map tasks)
├── mr-out-1
├── mr-out-2
└── mr-out-3    ← n_reduce output files total
```

## Crash Recovery

- Each `MapTask` and `ReduceTask` stores a `started_at: Option<Instant>`
- A background `task_timeout_checker` task runs every 5 seconds
- Any task `InProgress` for more than 10 seconds is reset to `Idle` and reassigned to the next available worker

## Status

- [x] Sequential word count (`mrsequential`)
- [x] Coordinator task assignment via RPC
- [x] Worker map phase (FNV-1a hash bucketing, intermediate file write)
- [x] Worker reduce phase (glob intermediate files, sort+count, output write)
- [x] Done notification (worker → coordinator) for both map and reduce
- [x] Crash recovery (task timeout + reassignment via background checker)
- [ ] Coordinator `Exit` signal to workers when all tasks done
