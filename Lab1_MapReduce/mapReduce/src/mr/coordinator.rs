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
    started_at: Option<std::time::Instant>,
}

pub struct ReduceTask {
    id: u32,
    status: TaskStatus,
    started_at: Option<std::time::Instant>,
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
        tasks.push(MapTask{id: i as u32, file: file.clone(), status: TaskStatus::Idle, started_at: None});
    }
    drop(tasks);
    for i in 0..n_reduce {
        reducetasks.lock().await.push(ReduceTask{id: i, status: TaskStatus::Idle, started_at: None});
    }

    let maptasks_checker = maptasks.clone();
    let reducetasks_checker = reducetasks.clone();
    tokio::spawn(task_timeout_checker(maptasks_checker, reducetasks_checker));

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

async fn task_timeout_checker(maptasks: Arc<Mutex<Vec<MapTask>>>, reducetasks: Arc<Mutex<Vec<ReduceTask>>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        let mut tasks = maptasks.lock().await;
        for task in tasks.iter_mut() {
            if task.status == TaskStatus::InProgress {
                if let Some(started) = task.started_at {
                    if started.elapsed().as_secs() > 10 {
                        println!("Map task {} timed out, reassigning", task.id);
                        task.status = TaskStatus::Idle;
                        task.started_at = None;
                    }
                }
            }
        }
        drop(tasks);
        let mut tasks = reducetasks.lock().await;
        for task in tasks.iter_mut() {
            if task.status == TaskStatus::InProgress {
                if let Some(started) = task.started_at {
                    if started.elapsed().as_secs() > 10 {
                        println!("Reduce task {} timed out, reassigning", task.id);
                        task.status = TaskStatus::Idle;
                        task.started_at = None;
                    }
                }
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
        if done.task_type == TaskType::Map {
            let mut tasks = maptasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == done.task_id) {
                task.status = TaskStatus::Done;
            }
            println!("Worker {} completed map task {}", worker_id, done.task_id);
            drop(tasks);
        }
        else if done.task_type == TaskType::Reduce {
            let mut tasks = reducetasks.lock().await;
            if let Some(task) = tasks.iter_mut().find(|t| t.id == done.task_id) {
                task.status = TaskStatus::Done;
            }
            println!("Worker {} completed reduce task {}", worker_id, done.task_id);
            drop(tasks);
        }
        return;
    }
    let mut tasks = maptasks.lock().await;
    let mut reducetasks = reducetasks.lock().await;

    let n_reduce = reducetasks.len();

    let all_maps_done = tasks.iter().all(|t| t.status == TaskStatus::Done);
    let reply: TaskArgs;
    if !all_maps_done {
        let task = tasks.iter_mut().find(|t| t.status == TaskStatus::Idle);
        reply = match task{
            Some(t) => {
                t.status = TaskStatus::InProgress;
                t.started_at = Some(std::time::Instant::now());
                println!("Assigning map task {} to worker {}", t.id, worker_id);
                TaskArgs { task_type: TaskType::Map, task_id: Some(t.id), input_file: Some(t.file.clone()), n_reduce: Some(n_reduce as u32) }
            }
            None => {
                TaskArgs { task_type: TaskType::Wait, task_id: None, input_file: None, n_reduce: None }
            }
        };
    }
    else{
        let task = reducetasks.iter_mut().find(|t| t.status == TaskStatus::Idle);
        reply = match task{
            Some(t) => {
                t.status = TaskStatus::InProgress;
                t.started_at = Some(std::time::Instant::now());
                println!("Assigning reduce task {} to worker {}", t.id, worker_id);
                TaskArgs { task_type: TaskType::Reduce, task_id: Some(t.id), input_file: None, n_reduce: Some(n_reduce as u32) }
            }
            None => {
                TaskArgs { task_type: TaskType::Wait, task_id: None, input_file: None, n_reduce: None }
            }
        };
    }
    drop(tasks);
    drop(reducetasks);
    


    let reply_bytes = serde_json::to_vec(&reply).unwrap();
    let reply_len = (reply_bytes.len() as u32).to_be_bytes();
    
    let _ = stream.write_all(&reply_len).await;
    let _ = stream.write_all(&reply_bytes).await;

}
