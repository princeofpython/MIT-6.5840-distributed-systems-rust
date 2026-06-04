use mapreduce::mr::coordinator::run_coordinator;

#[tokio::main]
async fn main() {
    println!("Coordinator starting...");
    //tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    run_coordinator().await;
    println!("Coordinator done.");
}
