use std::io::{self, Write};

use crate::TableCollection;

pub fn run_repl() {
    let mut table_collection = TableCollection::new();
    let mut stdout = io::stdout();
    let stdin = io::stdin();

    loop {
        stdout.write("> ".as_bytes()).unwrap();
        stdout.flush().unwrap();

        let mut buffer = String::new();
        stdin.read_line(&mut buffer).unwrap();

        let result = table_collection.query(&buffer).unwrap();
        result.write(&mut stdout).unwrap();
        stdout.flush().unwrap();
    }
}
