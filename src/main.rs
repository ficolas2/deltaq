use std::sync::Arc;

use deltalake::datafusion::prelude::SessionContext;
use deltalake::open_table;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    
    let mut rl = DefaultEditor::new()?;
    loop {
        let readline = rl.readline("deltaq> ");
        match readline {
            Ok(line) if matches!(line.trim(), "quit" | "exit" | "\\q") => break,
            Err(ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                println!("");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}

