#[tokio::main]
async fn main() {
    println!("Coordinator starting...");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    println!("Coordinator done.");
}
