use mapreduce::mr::coordinator::run_coordinator;

#[tokio::main]
async fn main() {
    println!("Coordinator starting...");

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <input_file> <n_reduce>", args[0]);
        return;
    }

    let input_folder = &args[1];
    let n_reduce = args[2].parse::<u32>().unwrap();

    let mut files:Vec<String> = Vec::new();
    for entry in std::fs::read_dir(input_folder).unwrap(){
        let path = entry.unwrap().path();
        files.push(path.to_string_lossy().to_string());
    }
    //tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    run_coordinator(files, n_reduce).await;
    println!("Coordinator done.");
}
