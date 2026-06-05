# Lab 1 — MapReduce

Distributed MapReduce implementation in Rust following the [MIT 6.5840 Lab 1 spec](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html).

## Architecture

- **Coordinator** — listens on TCP `127.0.0.1:7777`, assigns map/reduce tasks to workers, tracks task state (`Idle` → `InProgress` → `Done`)
- **Worker** — connects to coordinator, requests tasks, executes map logic, notifies coordinator on completion
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
cargo run --bin mrcoordinator -- ../inputs 3
cargo run --bin mrworker -- 1
cargo run --bin mrworker -- 2
```

## Map Phase

- Coordinator reads all input files from `<input_folder>`, creates one `MapTask` per file
- Worker receives a task (file path + `n_reduce`), reads the file, splits into words
- Each word is hashed with FNV-1a and assigned to a bucket: `bucket = fnv1a(word) % n_reduce`
- Output written to `intermediate/mr-{task_id}-{bucket}` (atomic: write to `.tmp` then rename)
- Worker notifies coordinator on completion; coordinator marks task `Done`

## Intermediate Files

```
intermediate/
├── mr-0-0   mr-0-1   mr-0-2    ← map task 0, buckets 0,1,2
├── mr-1-0   mr-1-1   mr-1-2    ← map task 1, buckets 0,1,2
└── ...
```

## Status

- [x] Sequential word count (`mrsequential`)
- [x] Coordinator task assignment via RPC
- [x] Worker map phase (hash bucketing, intermediate file write)
- [x] Done notification (worker → coordinator)
- [ ] Reduce phase
- [ ] Coordinator `Done()` / worker exit on completion
- [ ] Crash recovery (task timeout + reassignment)
