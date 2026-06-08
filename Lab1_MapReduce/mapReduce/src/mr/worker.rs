use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::mr::rpc::{TaskArgs, TaskRequest, TaskType, DoneNotify};
use crate::mr::coordinator::ADDR;

const OUTPUT_DIR: &str = "output";
const INTERMEDIATE_DIR: &str = "intermediate";

pub async fn run_worker(worker_id: u32) {
    loop{
        match task_request(worker_id).await {
            Ok(task) => {
                match task.task_type{
                    TaskType::Map =>{
                        println!("Worker {}: got Map task {:?}, file: {:?}", worker_id, task.task_id.unwrap(), task.input_file);
                        // map logic goes here
                        let task_id = task.task_id.unwrap();
                        run_map(task).await.unwrap();
                        notify_done(worker_id, TaskType::Map, task_id).await.unwrap();
                    }
                    TaskType::Reduce =>{
                        println!("Worker {}: got Reduce task {:?}", worker_id, task.task_id);
                        let task_id = task.task_id.unwrap();
                        run_reduce(task).await.unwrap();
                        notify_done(worker_id, TaskType::Reduce, task_id).await.unwrap();
                    }
                    TaskType::Wait =>{
                        println!("Worker {}: no task ready, waiting...", worker_id);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    TaskType::Exit => {
                        println!("Worker {}: all done, exiting.", worker_id);
                        return;
                    }
                }
            }
            Err(e) => {
                eprintln!("Worker {}: coordinator unreachable ({}), assuming done.", worker_id, e);
                return;
            }
        }
    }    
}

async fn task_request(worker_id: u32) -> Result<TaskArgs, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(ADDR).await?;

    let request = TaskRequest { worker_id, done: None };
    let request_bytes = serde_json::to_vec(&request)?;
    let request_len = (request_bytes.len() as u32).to_be_bytes();

    stream.write_all(&request_len).await?;
    stream.write_all(&request_bytes).await?;

    let mut response_len_bytes = [0u8; 4];
    stream.read_exact(&mut response_len_bytes).await?;
    let response_len = u32::from_be_bytes(response_len_bytes) as usize;

    let mut response_bytes = vec![0u8; response_len];
    stream.read_exact(&mut response_bytes).await?;

    let response: TaskArgs = serde_json::from_slice(&response_bytes)?;
    Ok(response)
}

async fn run_map(task: TaskArgs)-> Result<(), Box<dyn std::error::Error>> {
    tokio::fs::create_dir_all(INTERMEDIATE_DIR).await.unwrap();
    // map logic goes here
    if task.task_type != TaskType::Map {
        return Err("Invalid task type".into());
    }
    let task_id = task.task_id.unwrap();
    let n_reduce = task.n_reduce.unwrap();
    let input_file = task.input_file.unwrap();

    println!("running map task {}", task_id);
    
    let mut hash_buckets: Vec<Vec<(String, String)>> = vec![Vec::new(); n_reduce as usize];
    let content = tokio::fs::read_to_string(&input_file).await.unwrap();
    for word in content.split(|c: char| !c.is_alphabetic() && c != '\'') {
        let hash = fnv1a(word)% n_reduce;
        hash_buckets[hash as usize].push((word.to_string(), "1".to_string()));
    }

    for (i, bucket) in hash_buckets.iter().enumerate() {
        let file_path = format!("{}/mr-{}-{}", INTERMEDIATE_DIR, task_id, i);
        let tmp_path = format!("{}.tmp", file_path);
        let content = serde_json::to_string(bucket).unwrap();
        tokio::fs::write(&tmp_path, content).await.unwrap();
        tokio::fs::rename(&tmp_path, &file_path).await.unwrap();
        println!("Task {}: wrote {} key-value pairs to {}", task_id, bucket.len(), file_path);
    }
    println!("sleeping for 5 seconds");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}

fn fnv1a(s: &str) -> u32{
    let mut hash = 2166136261u32;
    for c in s.bytes() {
        hash = hash ^ (c as u32);
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

async fn notify_done(worker_id: u32, task_type: TaskType, task_id: u32)-> Result<(), Box<dyn std::error::Error>>{
    let mut stream = TcpStream::connect(ADDR).await?;

    let request = TaskRequest{
        worker_id,
        done: Some(DoneNotify {
            task_type,
            task_id,
        })
    };

    let request_bytes = serde_json::to_vec(&request)?;
    let request_len = (request_bytes.len() as u32).to_be_bytes();

    stream.write_all(&request_len).await?;
    stream.write_all(&request_bytes).await?;

    Ok(())
}

async fn run_reduce(task: TaskArgs)-> Result<(), Box<dyn std::error::Error>> {
    tokio::fs::create_dir_all(OUTPUT_DIR).await.unwrap();

    // reduce logic goes here
    if task.task_type != TaskType::Reduce {
        return Err("Invalid task type".into());
    }
    let task_id = task.task_id.unwrap();

    println!("running reduce task {}", task_id);
    
   let mut all_pairs: Vec<(String, String)> = Vec::new();
   let mut dir = tokio::fs::read_dir(INTERMEDIATE_DIR).await?;

   while let Some(entry) = dir.next_entry().await? {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        if file_name.ends_with(&format!("-{}", task_id)) && file_name.starts_with("mr-") {
            // read the file
            let content = tokio::fs::read_to_string(&entry.path()).await?;
            let pairs: Vec<(String, String)> = serde_json::from_str(&content)?;
            all_pairs.extend(pairs);
        }
   }

   all_pairs.sort_by(|a, b| a.0.cmp(&b.0));

   let mut output:String = String::new();

    let mut i = 0;
    while i < all_pairs.len(){
        let (key, _) = &all_pairs[i];
        let mut count = 0;
        while i < all_pairs.len() && all_pairs[i].0 == *key {
            count += 1;
            i += 1;
        }
        output.push_str(&format!("{} {}\n", key, count));
    }
    
    let output_path = format!("{}/mr-out-{}", OUTPUT_DIR, task_id);
    let tmp_path = format!("{}.tmp", output_path);
    tokio::fs::write(&tmp_path, &output).await?;
    tokio::fs::rename(&tmp_path, &output_path).await?;
    println!("sleeping for 5 seconds");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}
