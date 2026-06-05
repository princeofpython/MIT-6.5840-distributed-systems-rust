use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum TaskType {
    Map,
    Reduce,
    Wait,
    Exit,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DoneNotify {
    pub task_type: TaskType,
    pub task_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskArgs {
    pub task_type: TaskType,
    pub task_id: Option<u32>,
    pub input_file: Option<String>,
    pub n_reduce: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskRequest {
    pub worker_id: u32,
    pub done: Option<DoneNotify>,
}

