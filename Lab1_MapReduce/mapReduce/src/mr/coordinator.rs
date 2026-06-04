use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::mr::rpc::{TaskArgs, TaskType, DoneNotify, TaskRequest};

pub const ADDR: &str = "127.0.0.1:7777";
pub async fn run_coordinator() {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Coordinator listening on {}", ADDR);

    loop{
        match listener.accept().await{
            Ok((stream, _)) => {
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }

}

//[ 4 bytes: length ][ N bytes: JSON body ]
async fn handle_connection(mut stream: tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err(){
        return;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    
    let mut body_buf = vec![0u8; len];
    if stream.read_exact(&mut body_buf).await.is_err(){
        return;
    }
    
    let _req: TaskRequest = match serde_json::from_slice(&body_buf) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to parse task request: {}", e);
            return;
        }
    };
    
    let reply = TaskArgs { task_type: TaskType::Wait, task_id: None, input_file: None };
    let reply_bytes = serde_json::to_vec(&reply).unwrap();
    let reply_len = (reply_bytes.len() as u32).to_be_bytes();
    
    let _ = stream.write_all(&reply_len).await;
    let _ = stream.write_all(&reply_bytes).await;

}
