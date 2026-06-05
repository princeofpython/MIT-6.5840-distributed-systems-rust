use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::mr::rpc::{TaskArgs, TaskType, DoneNotify, TaskRequest};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(PartialEq)]
pub enum TaskStatus{
    Idle,
    InProgress,
    Done,
}

pub struct MapTask {
    id: u32,
    file: String,
    status: TaskStatus,
}

pub struct ReduceTask {
    id: u32,
    status: TaskStatus,
}

pub const ADDR: &str = "127.0.0.1:7777";
pub async fn run_coordinator(files: Vec<String>, n_reduce: u32) {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Coordinator listening on {}", ADDR);
    
    // next statement not work, because maptasks is not cloneable
    //let mut maptasks:Vec<MapTask> = vec![MapTask{id: 0, file: String::new(), status: TaskStatus::Idle}; files.len()];
    let maptasks: Arc<Mutex<Vec<MapTask>>> = Arc::new(Mutex::new(Vec::new()));
    let reducetasks: Arc<Mutex<Vec<ReduceTask>>> = Arc::new(Mutex::new(Vec::new()));
    //let mut reducetasks:Vec<ReduceTask> = vec![ReduceTask{id: 0, status: TaskStatus::Idle}; n_reduce as usize];

    let mut tasks = maptasks.lock().await;
    for (i, file) in files.iter().enumerate(){
        tasks.push(MapTask{id: i as u32, file: file.clone(), status: TaskStatus::Idle});
    }
    drop(tasks);
    for i in 0..n_reduce {
        reducetasks.lock().await.push(ReduceTask{id: i, status: TaskStatus::Idle});
    }

    loop{
        match listener.accept().await{
            Ok((stream, _)) => {
                let maptasks_clone = maptasks.clone();
                let reducetasks_clone = reducetasks.clone();
                tokio::spawn(handle_connection(stream, maptasks_clone, reducetasks_clone));
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }

}

//[ 4 bytes: length ][ N bytes: JSON body ]
async fn handle_connection(mut stream: tokio::net::TcpStream, maptasks: Arc<Mutex<Vec<MapTask>>>, reducetasks: Arc<Mutex<Vec<ReduceTask>>>) {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err(){
        return;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    
    let mut body_buf = vec![0u8; len];
    if stream.read_exact(&mut body_buf).await.is_err(){
        return;
    }
    
    let req: TaskRequest = match serde_json::from_slice(&body_buf) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to parse task request: {}", e);
            return;
        }
    };
    let worker_id = req.worker_id;
    if let Some(done) = req.done{
        // handle done notification
        let mut tasks = maptasks.lock().await;
        if let Some(task) = tasks.iter_mut().find(|t| t.id == done.task_id) {
            task.status = TaskStatus::Done;
        }
        println!("Worker {} completed map task {}", worker_id, done.task_id);
        drop(tasks);
        return;
    }
    let mut tasks = maptasks.lock().await;
    let n_reduce = reducetasks.lock().await.len();
    let task = tasks.iter_mut().find(|t| t.status == TaskStatus::Idle);
    let reply = match task{
        Some(t) => {
            t.status = TaskStatus::InProgress;
            println!("Assigning map task {} to worker {}", t.id, worker_id);
            TaskArgs { task_type: TaskType::Map, task_id: Some(t.id), input_file: Some(t.file.clone()), n_reduce: Some(n_reduce as u32) }
        }
        None => {
            TaskArgs { task_type: TaskType::Wait, task_id: None, input_file: None, n_reduce: None }
        }
    };
    drop(tasks);

    let reply_bytes = serde_json::to_vec(&reply).unwrap();
    let reply_len = (reply_bytes.len() as u32).to_be_bytes();
    
    let _ = stream.write_all(&reply_len).await;
    let _ = stream.write_all(&reply_bytes).await;

}
