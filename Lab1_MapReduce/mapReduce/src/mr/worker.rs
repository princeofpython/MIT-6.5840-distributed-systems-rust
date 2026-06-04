use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::mr::rpc::{TaskArgs, TaskRequest, TaskType};
use crate::mr::coordinator::ADDR;

pub async fn run_worker(worker_id: u32) {
    loop{
        match task_request(worker_id).await {
            Ok(task) => {
                match task.task_type{
                    TaskType::Map =>{
                        println!("Worker {}: got Map task {:?}, file: {:?}", worker_id, task.task_id, task.input_file);
                        // map logic goes here
                    }
                    TaskType::Reduce =>{
                        println!("Worker {}: got Reduce task {:?}", worker_id, task.task_id);
                        // reduce logic goes here
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

    let request = TaskRequest { worker_id };
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