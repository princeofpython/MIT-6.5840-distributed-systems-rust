use std::collections::HashMap;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input_folder>", args[0]);
        return Ok(());
    }
    let mut word_map = HashMap::new();

    let input_folder = &args[1];
    for file in std::fs::read_dir(Path::new(input_folder))?{
        let file_path = file?.path();
        if file_path.is_file(){
            let file_data = std::fs::read_to_string(file_path)?;
            count_words_in_file(&file_data, &mut word_map);
        }
    }
    let mut items: Vec<_> = word_map.iter().collect();
    items.sort_by_key(|(word, _)| *word);

    let mut output = String::new();
    for (word, count) in items{        
        output.push_str(&format!("{} : {}\n", word, count));
    }
    std::fs::write("output/seq-out.txt", output)?;
    Ok(())
}

fn count_words_in_file(file_data: &str, word_map: &mut HashMap<String, u32>){
    //let words = file_data.split_whitespace();
    for word in file_data.split(|c:char| !c.is_alphabetic() && c != '\''){
        if !word.is_empty(){
            *word_map.entry(word.to_string()).or_insert(0) += 1;
        }
    }
}
