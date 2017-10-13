extern crate rayon;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
// use std::collections::BTreeMap;
use std::collections::HashMap;
use rayon::prelude::*;

fn sanitize_word(input: &str) -> String {
    let ret: String = input.chars().filter(|c| c.is_alphabetic()).collect();
    ret.to_lowercase()
}

fn word_frequency(path: &str) -> HashMap<String, u32> {
    let file = File::open(path).expect("Error opening file");
    //  let file = File::open("/home/cs-unsax/Documents/ol_dump_works_2017-08-31.txt").expect("Error opening file");
    let reader = BufReader::new(file);

    // let mut frequency: BTreeMap<String, u32> = BTreeMap::new();
    let mut frequency: HashMap<String, u32> = HashMap::new();

    // for word in reader.split(b' ') {
    //     if let Ok(word) = word {
    //         *frequency.entry(sanitize_word(word as String)).or_insert(0) += 1;
    //     } else {
    //         println!("Error reading line");
    //     }
    // }

    for line in reader.lines() {
        if let Ok(line) = line {
            for word in line.split_whitespace() {
                *frequency.entry(sanitize_word(word)).or_insert(0) += 1;
            }
        } else {
            println!("Error reading line");
        }
    }

    frequency
}

fn main() {
    // let frequency = word_frequency("data/BOM.txt");
    // let input = vec!["data/BOM.txt", "data/file2.txt"];
    let input = vec!["/home/cs-unsax/Documents/30gb.txt",
                    //  "/home/cs-unsax/Documents/dataols01",
                    //  "/home/cs-unsax/Documents/dataols02",
                    //  "/home/cs-unsax/Documents/dataols03",
                     "/home/cs-unsax/Documents/ol_dump_works_2017-08-31.txt"];

    let maps: Vec<HashMap<String, u32>> = input.par_iter().map(|file| word_frequency(file)).collect();
    let mut frequency: HashMap<String, u32> = HashMap::new();

    for map in &maps {
        for (word, count) in map {
            *frequency.entry(sanitize_word(word)).or_insert(0) += *count;
        }
    }

    let mut ofile = File::create("Dictionary2.txt").expect("Couldn't open write file");
    write!(ofile, "{:#?}\n", frequency).expect("Couldn't write in file");
    // println!("{:?}", frequency);
}
