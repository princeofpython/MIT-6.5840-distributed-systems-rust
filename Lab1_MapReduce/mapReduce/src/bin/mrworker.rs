use mapreduce::mr::worker::run_worker;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let worker_id = args[1].parse::<u32>().unwrap();
    println!("Worker starting...");
    //tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    run_worker(worker_id).await;
    println!("Worker done.");
}